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
import java.util.concurrent.atomic.AtomicInteger;

public class ComputeNode implements Node {

    private Logger log = Logger.getLogger(this.getClass().getName());
    private ServerSocket serverSocket;
    private boolean running = true;
    private int numThreads;

    private NodeID registryNode;
    public NodeID myNode;
    private NodeID successorID;
    public TCPConnection registryConn;

    private Map<NodeID, TCPConnection> connections = new ConcurrentHashMap<>();
    private Map<Socket, TCPConnection> socketToConn = new ConcurrentHashMap<>();
    private volatile List<NodeID> connectionList = new ArrayList<>();
    private Set<UUID> seenRequests = ConcurrentHashMap.newKeySet();
    private Set<UUID> sentRequests = ConcurrentHashMap.newKeySet();

    private TaskProcessor processor;

    // metrics
    private AtomicInteger pulledTasks = new AtomicInteger(0);
    private AtomicInteger pushedTasks = new AtomicInteger(0);


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
        }
        else if(event.getType() == Protocol.MESSAGING_NODES_LIST) {
            MessagingNodesList message = (MessagingNodesList) event;
            log.info("Received connection list from Registry..." + "\n\tConnecting to " + message.numConnections + " nodes.");
            connectionList = message.getPeers();
            successorID = connectionList.get(0);
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
            processor = new TaskProcessor(numThreads, myNode, this);
        }
        else if(event.getType() == Protocol.TASK_SUM){
            TaskSum message = (TaskSum) event;
            if (processor.completedTaskSumNodes.add(message.nodeId)) {
                processor.networkTaskSum.addAndGet(message.taskSum);
                try {
                    forwardMessage(message.getBytes());
                } catch(IOException e) {
                    log.warning("Exception while forwarding task sum message..." + e.getStackTrace());
                }
            }
            if(processor.completedTaskSumNodes.size() == processor.totalNumRegisteredNodes.get()) {
                printNetworkTaskSum();
                processor.completedTaskSumNodes.clear();
                processor.computeLoadBalancing();
                processor.phase.set(Phase.PROCESSING);
                processor.createThreadPool(numThreads);
            }
        }
        else if(event.getType() == Protocol.TASK_INITIATE){
            TaskInitiate ti = (TaskInitiate) event;
            log.info("Received task initiate from Registry with " + ti.numRounds + " rounds...");
            processor.setNumRounds(ti.numRounds);
            processor.createTasks(ti.numRounds);
            processor.printTasksInQueue();
            sendTaskSum();
        }
        else if(event.getType() == Protocol.TASK_REQUEST) {
            // log.info("Received task request...");
            TaskRequest request = (TaskRequest) event;
            handleTaskRequest(request, socket);
        }
        else if(event.getType() == Protocol.TASK_RESPONSE) {
            // log.info("Received task repsonse...");
            TaskResponse response = (TaskResponse) event;
            handleTaskResponse(response, socket);
        }
        else if(event.getType() == Protocol.PULL_TRAFFIC_SUMMARY) {
            try {
                if(processor.excessQueue.size() > 0 || processor.taskQueue.size() > 0){
                    processor.drainQueues();
                }
                //log.info("Received task summary request from Registry. Sending back requested information...");
                TaskSummaryResponse response = new TaskSummaryResponse(Protocol.TRAFFIC_SUMMARY, myNode.getPort(), processor.getTotalTasks(), pulledTasks.get(), pushedTasks.get(), processor.tasksCompleted.get(), processor.calculatePercentageOfWork());
                registryConn.sender.sendData(response.getBytes());
            } catch(IOException e) {
                log.warning("Exception while sending TaskSummaryReport to Registry...." + e.getMessage());
            }
        }
    }

    public void checkForLoadBalancing() {
        if(processor.phase.get() == Phase.PROCESSING && processor.taskQueue.isEmpty() && processor.remainingTasksNeeded() > 0) {
            if(processor.pendingRequests.compareAndSet(0, 1)) {
                log.info("Queue is empty and there are no pending reqeusts. Sending new taks request...");
                processor.phase.set(Phase.LOAD_BALANCE);
                initiateTaskRequest(processor.remainingTasksNeeded());
            }
        }
    }

    private void handleTaskResponse(TaskResponse response, Socket incoming){
        try {
            NodeID requester = response.requesterId;
            if(requester.equals(myNode)){ // this is for me 
                log.info("Received a response with " + response.tasks.size() + " tasks.");
                if(processor.phase.compareAndSet(Phase.LOAD_BALANCE, Phase.PROCESSING)) {
                    log.info("State is LOAD_BALANCE. Accepting tasks and returning to PROCESSING.");
                    pulledTasks.incrementAndGet();
                    processor.taskQueue.addAll(response.tasks);
                    processor.pendingRequests.decrementAndGet();
                } else {
                    log.warning("Received tasks but not in LOAD_BALANCE state. Current state: " + processor.phase.get());
                }
                if(done()){
                    log.info("All tasks complete. Setting phase to DONE...");
                    processor.phase.set(Phase.DONE);
                }
            } else {
                //log.info("Forwarding response for requester " + requester + " to successor " + successorID);
                forwardMessage(response.getBytes());
            }
        }catch(IOException e) {
            log.info("Exception while handling task response..." + e.getStackTrace());
            Thread.currentThread().interrupt();
        }
    }

    private boolean done() {
        return processor.remainingTasksNeeded() == 0 && processor.taskQueue.isEmpty() && processor.tasksBeingMined.get() == 0 && processor.pendingRequests.get() == 0;
    }

    private void handleTaskRequest(TaskRequest request, Socket incoming) {
        try {
            List<Task> tasks = new ArrayList<>();
            NodeID requester = request.requesterId;
            UUID uuid = request.uuid;

            seenRequests.add(uuid);
            if (!processor.excessQueue.isEmpty()) { // I have tasks to give 
                int amountToGive = Math.min(request.numTasksRequested, processor.excessQueue.size());

                log.info("Fulfilling request from " + requester + ", Sending: " + amountToGive);
                pushedTasks.incrementAndGet();
                for (int i = 0; i < amountToGive; i++) {
                    Task task = processor.excessQueue.poll();
                    if (task != null) {
                        tasks.add(task);
                    }
                }
                
                TaskResponse response = new TaskResponse(Protocol.TASK_RESPONSE, requester, tasks);
                TCPConnection conn = connections.get(requester);
                if(conn != null) {
                    conn.sender.sendData(response.getBytes());
                } else {
                    forwardMessage(response.getBytes());
                }
            } else { // I dont have tasks to give
                //log.info("Forwarding request " + uuid + " to successor " + successorID + " with TTL " + request.ttl);
                request.ttl -= 1;
                forwardMessage(request.getBytes());
            }
        } catch (IOException e) {
            log.warning("Exception while handling task request." + e);
        }
    }

    private void forwardMessage(byte[] message) {
        try{
            TCPConnection conn = connections.get(successorID);
            if(conn != null) {
                conn.sender.sendData(message);
            } else {
                log.warning("Error when forwarding message. Could not find connection for successor ID: " + successorID.toString());
            }
        }catch(IOException e) {
            log.warning("Exception while forwarding message..." + e.getStackTrace());
        }
    }

    private void initiateTaskRequest(int requestedTasks) {
        log.info("Sending task request for " + requestedTasks + " tasks...");
        try {
            UUID uuid = UUID.randomUUID();
            TaskRequest tr = new TaskRequest(Protocol.TASK_REQUEST, myNode, requestedTasks, processor.totalNumRegisteredNodes.get() - 1, uuid);
            forwardMessage(tr.getBytes());
            sentRequests.add(uuid);
        } catch(IOException e) {
            log.warning("Exception while initiating a task request..." + e.getStackTrace());
        }
    }

    public void printNetworkTaskSum() {
        log.info("Total number of tasks in the network: " + Integer.toString(processor.networkTaskSum.get()));
    }

    private void sendTaskSum(){
        try{
            log.info("Sending task sum to other nodes...");
            int taskSum = processor.getTotalTasks();
            TaskSum taskMessage = new TaskSum(Protocol.TASK_SUM, taskSum, myNode);
            forwardMessage(taskMessage.getBytes());
            if (processor.completedTaskSumNodes.add(myNode)) processor.networkTaskSum.addAndGet(taskSum);
        } catch(IOException e) {
            log.warning("Exception while send task sum to other nodes..." + e.getStackTrace());
        }
    }

    private void register() {
        try{
            Socket socket = new Socket(registryNode.getIP(), registryNode.getPort());
            registryConn = new TCPConnection(socket, this);
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
                    case "print-task-info":
                        processor.printTaskInfo();
                        break;
                    case "print-pending":
                        processor.printPendingRequests();
                        break;
                    case "print-requests":
                        printRequests();
                        break;
                    case "print-phase":
                        processor.printPhase();
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

    private void printRequests(){
        log.info("Sent request UUIDs: ");
        for(UUID uuid : sentRequests) {
            log.info(uuid.toString());
        }
        log.info("Received reques UUIDs: ");
        for(UUID uuid : seenRequests){
            log.info(uuid.toString());
        }
    }

    public static void main(String[] args) {

        LogConfig.init(Level.INFO);
        ComputeNode node = new ComputeNode(args[0], Integer.parseInt(args[1]));
        new Thread(node::startNode, "Node-" + node.toString() + "-Server").start();
        new Thread(node::readTerminal, "Node-" + node.toString() + "-Terminal").start();
        
    }
    
}
