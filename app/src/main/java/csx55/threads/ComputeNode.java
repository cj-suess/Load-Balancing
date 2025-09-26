package csx55.threads;

import java.io.IOException;
import java.net.*;

import csx55.hashing.Task;
import csx55.transport.TCPConnection;
import csx55.util.LogConfig;
import csx55.util.TaskProcessor;
import csx55.util.TaskProcessor.Phase;
import csx55.wireformats.*;
import java.util.logging.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ComputeNode implements Node {

    private Logger log = Logger.getLogger(this.getClass().getName());
    private ServerSocket serverSocket;
    private boolean running = true;
    private int numThreads;

    private NodeID registryNode;
    private NodeID myNode;

    private Map<NodeID, TCPConnection> connections = new ConcurrentHashMap<>();
    private Map<Socket, TCPConnection> socketToConn = new ConcurrentHashMap<>();
    private volatile List<NodeID> connectionList = new ArrayList<>();

    private TaskProcessor processor;


    public ComputeNode(String host, int port) {
        registryNode = new NodeID(host, port);
    }

    @Override
    public void onEvent(Event event, Socket socket) {
        if(event == null) {
            log.warning("Null event received from Event Factory...");
        }
        else if(event.getType() == Protocol.REGISTER_RESPONSE) {
            log.info("Received register response from Registry...");
            Message message = (Message) event; 
            System.out.println(message.info);
        }
        else if(event.getType() == Protocol.NODE_ID){
            log.info("Receiving nodeID information...");
            Message message = (Message) event;
            String info = message.info;
            NodeID node = new NodeID(info.substring(0, info.indexOf(':')), Integer.parseInt(info.substring(info.indexOf(':') + 1)));
            TCPConnection conn = socketToConn.get(socket);
            connections.put(node, conn);
            connectionList.add(node);
        }
        else if(event.getType() == Protocol.MESSAGING_NODES_LIST) {
            MessagingNodesList message = (MessagingNodesList) event;
            log.info("Received connection list from Registry..." + "\n\tConnecting to " + message.numConnections + " nodes.");
            connectionList = message.getPeers();
            connect();
        }
        else if(event.getType() == Protocol.TOTAL_NUM_NODES){
            Message message = (Message) event;
            processor.totalNumRegisteredNodes.getAndSet(Integer.parseInt(message.info));
            log.info("Received the total number of nodes in the network...." + "\n\tTotal number of nodes: " + processor.totalNumRegisteredNodes.get());
        }
        else if(event.getType() == Protocol.THREADS){
            Message message = (Message) event;
            numThreads = Integer.parseInt(message.info);
            log.info("Recieving thread count from Registry...\n" + "\tThread Count :" + numThreads);
            processor = new TaskProcessor(numThreads, myNode);
        }
        else if(event.getType() == Protocol.TASK_SUM){
            TaskSum message = (TaskSum) event;
            if (processor.completedTaskSumNodes.add(message.nodeId)) {
                processor.networkTaskSum.addAndGet(message.taskSum);
                relayTaskSum(message, socket);
            }
            if(processor.completedTaskSumNodes.size() == processor.totalNumRegisteredNodes.get()) {
                printNetworkTaskSum();
                processor.completedTaskSumNodes.clear();
                processor.computeLoadBalancing();
                processor.phase.set(Phase.LOCAL);
                processor.createThreadPool(numThreads);
                while(processor.phase.get() != Phase.DONE) {
                    checkForLoadBalancing(processor);
                }
            }
        }
        else if(event.getType() == Protocol.TASK_INITIATE){
            TaskInitiate ti = (TaskInitiate) event;
            log.info("Received task initiate from Registry with " + ti.numRounds + " rounds...");
            processor.createTasks(ti.numRounds);
            processor.printTasksInQueue();
            sendTaskSum();
        }
        else if(event.getType() == Protocol.TASK_REQUEST) {
            log.info("Received task request...");
            TaskRequest request = (TaskRequest) event;
            handleTaskRequest(request, socket);
        }
        else if(event.getType() == Protocol.TASK_RESPONSE) {
            log.info("Received task repsonse...");
            TaskResponse response = (TaskResponse) event;
            handleTaskResponse(response, socket);
        }
    }

    private void checkForLoadBalancing(TaskProcessor processor) {
        if(processor.phase.get() == Phase.LOCAL && processor.taskQueue.isEmpty() && processor.remainingTasksNeeded() > 0) {
            if(processor.phase.compareAndSet(Phase.LOCAL, Phase.LOAD_BALANCE)) {
                processor.pendingRequests.incrementAndGet();
                initiateTaskRequest(processor.remainingTasksNeeded());
            }
        }
    }

    private void handleTaskResponse(TaskResponse response, Socket incoming){
        try {
            processor.pendingRequests.decrementAndGet();
            NodeID requester = response.requesterId;
            if(requester.equals(myNode)){ // this is for me 
                log.info("Received response with " + response.tasks.size() + " tasks.");
                if(processor.phase.compareAndSet(Phase.LOAD_BALANCE, Phase.LOCAL)) {
                    processor.taskQueue.addAll(response.tasks);
                }
                // ask for more if needed
                if(processor.remainingTasksNeeded() > 0) {
                    if(processor.phase.compareAndSet(Phase.LOCAL, Phase.LOAD_BALANCE)) {
                        initiateTaskRequest(processor.remainingTasksNeeded());
                    }
                }
                if(done()){
                    log.info("Setting phase to DONE...");
                    processor.phase.set(Phase.DONE);
                }
            } else {
                log.info("Forwarding message...");
                forwardMessage(response.getBytes(), incoming);
            }
        }catch(IOException e) {
            log.info("Exception while handling task response..." + e.getStackTrace());
            Thread.currentThread().interrupt();
        }
    }

    private boolean done() {
        return processor.remainingTasksNeeded() == 0 && processor.taskQueue.isEmpty() && processor.tasksBeingMined.get() == 0 && processor.pendingRequests.get() == 0;
    }

    private void handleTaskRequest(TaskRequest request, Socket incoming){
        try {

            TaskResponse response;
            List<Task> tasks = new ArrayList<>();
            NodeID requester = request.requesterId;
            int neededTasks = processor.remainingTasksNeeded();
            int availableToSend = Math.max(0, processor.taskQueue.size() - neededTasks);
            int tasksToSend = Math.min(request.numTasksRequested, availableToSend);

            if(processor.remainingTasksNeeded() == 0) {
                log.info("Can fulfill request for " + request.requesterId);
                for(int i = 0; i < tasksToSend; i++) {
                    Task task = processor.taskQueue.poll();
                    if(task != null) {
                        tasks.add(task);
                    }
                }
                response = new TaskResponse(Protocol.TASK_RESPONSE, requester, tasks);
                TCPConnection requestConn = connections.get(requester);
                if(requestConn != null) {
                    requestConn.sender.sendData(response.getBytes());
                } else {
                    forwardMessage(response.getBytes(), incoming);
                }
            } else {
                log.info("Cannot fulfill request at this time. Forwarding request.");
                forwardMessage(request.getBytes(), incoming);
            }
        } catch(IOException e) {
            log.warning("Exception while handling task request..." + e.getStackTrace());
        }
    }

    private void forwardMessage(byte[] message, Socket incoming) {
        try{
            for(Map.Entry<Socket, TCPConnection> entry : socketToConn.entrySet()){
                if(!entry.getKey().equals(incoming)){
                    entry.getValue().sender.sendData(message);
                    return;
                }
            }
        }catch(IOException e) {
            log.warning("Exception while forwarding message..." + e.getStackTrace());
        }
    }

    private void initiateTaskRequest(int requestedTasks) {
        log.info("Sending task request for " + requestedTasks + " tasks...");
        try {
            List<NodeID> activeConns = new ArrayList<>(connections.keySet());
            if(activeConns.isEmpty()){
                log.warning("Cannot request tasks. No active connections.");
                return;
            }
            TaskRequest tr = new TaskRequest(Protocol.TASK_REQUEST, myNode, requestedTasks);
            NodeID neighbor = activeConns.get(new Random().nextInt(activeConns.size()));
            connections.get(neighbor).sender.sendData(tr.getBytes());
        } catch(IOException e) {
            log.warning("Exception while initiating a task request..." + e.getStackTrace());
        }
    }
    
    private void relayTaskSum(TaskSum message, Socket incoming){
        for(Map.Entry<Socket, TCPConnection> entry : socketToConn.entrySet()){
            Socket socket = entry.getKey();
            if(!socket.equals(incoming)){
                try {
                    entry.getValue().sender.sendData(message.getBytes());
                } catch(IOException e) {
                    log.warning("Exception while relaying task sum message..." + e.getStackTrace());
                }
            }
        }
    }

    public void printNetworkTaskSum(){
        log.info("Total number of tasks in the network: " + Integer.toString(processor.networkTaskSum.get()));
    }

    private void sendTaskSum(){
        try{
            log.info("Sending task sum to other nodes...");
            int taskSum = processor.getTotalTasks();
            TaskSum taskMessage = new TaskSum(Protocol.TASK_SUM, taskSum, myNode);
            for(Map.Entry<NodeID, TCPConnection> entry : connections.entrySet()) {
                entry.getValue().sender.sendData(taskMessage.getBytes());
            }
            if (processor.completedTaskSumNodes.add(myNode)) processor.networkTaskSum.addAndGet(taskSum);
        } catch(IOException e) {
            log.warning("Exception while send task sum to other nodes..." + e.getStackTrace());
        }
    }

    private void register() {
        try{
            Socket socket = new Socket(registryNode.getIP(), registryNode.getPort());
            TCPConnection registryConn = new TCPConnection(socket, this);
            Register registerMessage = new Register(Protocol.REGISTER_REQUEST, myNode);
            registryConn.startReceiverThread();
            registryConn.sender.sendData(registerMessage.getBytes());
        } catch(IOException e) {
            log.warning("Exception thrown while registering node with Registry..." + e.getStackTrace().toString());
        }

    }

    private void connect() {
        for(NodeID node : connectionList) {
            try {
                Socket socket = new Socket(node.getIP(), node.getPort());
                TCPConnection conn = new TCPConnection(socket, this);
                socketToConn.put(socket, conn);
                connections.put(node, conn);
                conn.startReceiverThread();

                Message idMessage = new Message(Protocol.NODE_ID, (byte)0, this.myNode.toString());
                conn.sender.sendData(idMessage.getBytes());
            } catch(IOException e) {
                log.warning("Exception while trying to connect to other compute nodes..." + e.getStackTrace());
            }
        }
    }

    private void startNode() {
        try {
            serverSocket = new ServerSocket(0);
            myNode = new NodeID(InetAddress.getLocalHost().getHostAddress(), serverSocket.getLocalPort());
            log = Logger.getLogger(ComputeNode.class.getName() + "[" + myNode.toString() + "]");
            register();
            while(running) {
                Socket clientSocket = serverSocket.accept();
                log.info("New connection from: " + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort());
                TCPConnection conn = new TCPConnection(clientSocket, this);
                conn.startReceiverThread();
                socketToConn.put(clientSocket, conn);
            }
        }catch(IOException e) {
            log.warning("Exception in startNode...." + e.getStackTrace());
        }
    }

    private void readTerminal() {
        try(Scanner scanner = new Scanner(System.in)) {
            while(true) {
                String command = scanner.nextLine();
                switch (command) {
                    case "print-connections":
                        printConnections();
                        break;
                    case "print-connection-list":
                        printConnectionList();
                        break;
                    case "print-total-tasks":
                        printNetworkTaskSum();
                        break;
                    case "print-tasks-in-queue":
                        processor.printTasksInQueue();
                        break;
                    default:
                        break;
                }
            }
        } 
    }

    private void printConnections() {
        for(Map.Entry<NodeID, TCPConnection> entry : connections.entrySet()){
            log.info(entry.getKey().toString());
        }
    }

    private void printConnectionList() {
        for(NodeID entry : connectionList){
            log.info(entry.toString());
        }
    }

    public static void main(String[] args) {

        LogConfig.init(Level.INFO);
        ComputeNode node = new ComputeNode(args[0], Integer.parseInt(args[1]));
        new Thread(node::startNode, "Node-" + node.toString() + "-Server").start();
        new Thread(node::readTerminal, "Node-" + node.toString() + "-Terminal").start();
        
    }
    
}
