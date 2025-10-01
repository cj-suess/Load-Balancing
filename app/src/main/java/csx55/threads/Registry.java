package csx55.threads;

import csx55.transport.*;
import csx55.util.*;
import csx55.wireformats.*;
import java.net.*;
import java.util.logging.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;

public class Registry implements Node {

    private Logger log = Logger.getLogger(this.getClass().getName());
    private int port;
    private ServerSocket serverSocket;
    private boolean running = true;

    List<TCPConnection> openConnections = new ArrayList<>();
    private Map<NodeID, TCPConnection> nodeToConnMap = new ConcurrentHashMap<>();
    private Map<NodeID, List<NodeID>> overlay = new ConcurrentHashMap<>();
    private Map<NodeID, List<NodeID>> connectionMap = new ConcurrentHashMap<>();
    private Set<NodeID> finishedNodes = Collections.newSetFromMap(new ConcurrentHashMap<>()); 
    private Map<String, List<Float>> summaryReport = new ConcurrentHashMap<>();

    public Registry(int port) {
        this.port = port;
    }
    
    @Override
    public void onEvent(Event event, Socket socket) {
        TCPSender sender = getSender(openConnections, socket);
        if(event.getType() == Protocol.REGISTER_REQUEST) {
            log.info("Register request detected. Checking status...");
            Register node = (Register) event;
            if (!nodeToConnMap.containsKey(node.nodeID)) {
                sendRegisterSuccess(node, sender, socket);
            }
            else {
                sendRegisterFailure(node, sender);
            }
        }
        else if(event.getType() == Protocol.TASK_COMPLETE) {
            TaskComplete taskComplete = (TaskComplete) event;
            NodeID nodeId = new NodeID(taskComplete.ip, taskComplete.port);
            // mark node as complete
            log.info("Received task complete message from " + nodeId);
            finishedNodes.add(nodeId);
            summaryReport.put(nodeId.toString(), new ArrayList<>());
            if(finishedNodes.size() == openConnections.size()) {
                summaryReport.put("sum", new ArrayList<>(Collections.nCopies(5, 0f)));
                finishedNodes.clear();
                sendTrafficSummaryRequest();
            }
        }
        else if(event.getType() == Protocol.TRAFFIC_SUMMARY) {
            TaskSummaryResponse tsr = (TaskSummaryResponse) event;
            addToSummaryReport(tsr, socket.getInetAddress().getHostAddress());

            if(finishedNodes.size() == openConnections.size()) {
                printSummaryReport();
            }
        }
    }

    private void printSummaryReport() {
        for(Map.Entry<String, List<Float>> entry : summaryReport.entrySet()){
            if(!entry.getKey().equals("sum")) {
                System.out.printf(entry.getKey() + " %.0f %.0f %.0f %.0f %.8f\n", entry.getValue().get(0), entry.getValue().get(1), entry.getValue().get(2), entry.getValue().get(3), entry.getValue().get(4) * 100);
            }
        }
        if(summaryReport.containsKey("sum")){
            System.out.printf("Total %.0f %.0f %.0f %.0f %.0f", summaryReport.get("sum").get(0), summaryReport.get("sum").get(1), summaryReport.get("sum").get(2), summaryReport.get("sum").get(3), summaryReport.get("sum").get(4) * 100);
        }
    }

    private synchronized void addToSummaryReport(TaskSummaryResponse tsr, String socketAddress) {

        String nodeIDString = socketAddress + ":" + tsr.serverPort;
        summaryReport.get(nodeIDString).add((float) tsr.generatedTasks);
        summaryReport.get(nodeIDString).add((float) tsr.pulledTasks);
        summaryReport.get(nodeIDString).add((float) tsr.pushedTasks);
        summaryReport.get(nodeIDString).add((float) tsr.completedTasks);
        summaryReport.get(nodeIDString).add(tsr.workloadPercentage);

        List<Float> sum = summaryReport.get("sum");
        sum.set(0, sum.get(0) + tsr.generatedTasks);
        sum.set(1, sum.get(1) + tsr.pulledTasks);
        sum.set(2, sum.get(2) + tsr.pushedTasks);
        sum.set(3, sum.get(3) + tsr.completedTasks);
        sum.set(4, sum.get(4) + tsr.workloadPercentage);

        NodeID nodeId = new NodeID(socketAddress, tsr.serverPort);
        finishedNodes.add(nodeId);
    }

    private void sendTrafficSummaryRequest() {
        try {
            log.info("Sending request for traffic summary to messaging nodes...");
            TaskSummaryRequest tsr = new TaskSummaryRequest(Protocol.PULL_TRAFFIC_SUMMARY);
            for(TCPConnection conn : openConnections) {
                conn.sender.sendData(tsr.getBytes());
            }
        } catch(IOException e) {
            log.warning("IOException while sending traffic summary request to nodes..." + e.getStackTrace());
        }
    }

    private void sendRegisterSuccess(Register node, TCPSender sender, Socket socket) {
        try {
            TCPConnection conn = getConnectionBySocket(openConnections, socket);
            nodeToConnMap.put(node.nodeID, conn);
            log.info(() -> "New node was added to the registry successfully!\n" + "\tCurrent number of nodes in registry: " + nodeToConnMap.size());
            String info = "Registration request successful. The number of messaging nodes currently constituting the overlay is (" + nodeToConnMap.size() + ")";
            Message successMessage = new Message(Protocol.REGISTER_RESPONSE, (byte)0, info);
            log.info(() -> "Sending success response to messaging node at " + node.nodeID.toString());
            sender.sendData(successMessage.getBytes());
        } catch(IOException e) {
            log.warning("Exception while registering compute node..." + e.getStackTrace());
        }
    }

    private void sendRegisterFailure(Register node, TCPSender sender) {
        try {
            if(nodeToConnMap.containsKey(node.nodeID)) {
                log.info(() -> "Cannot register node. A matching nodeID already exists in the registry...");
                String info = "Registration request failed.";
                Message failureMessage = new Message(Protocol.REGISTER_RESPONSE, (byte)1, info);
                sender.sendData(failureMessage.getBytes());
            }
        } catch(IOException e) {
            log.warning("Exception while registering compute node..." + e.getStackTrace());
        }
    }

    private void startRegistry() {
        try {
            serverSocket = new ServerSocket(port);
            log.info("Registry is up and running. Listening on port: " + port);
            while(running) {
                Socket clientSocket = serverSocket.accept();
                InetSocketAddress client = (InetSocketAddress) clientSocket.getRemoteSocketAddress();
                log.info("New connection from: " + client.getAddress() + ":" + client.getPort());
                TCPConnection conn = new TCPConnection(clientSocket, this);
                conn.startReceiverThread();
                openConnections.add(conn);
            }
        } catch(IOException e) {
            log.warning("Exception while accepting connections..." + e.getStackTrace());
        }
    }

    private TCPSender getSender(List<TCPConnection> openConnections, Socket socket) {
        TCPSender sender = null;
        for(TCPConnection conn : openConnections) {
            if(socket == conn.socket) {
                sender = conn.sender;
            }
        }
        return sender;
    }

    private TCPConnection getConnectionBySocket(List<TCPConnection> openConnections, Socket socket) {
        for(TCPConnection conn : openConnections) {
            if(conn.socket == socket) {
                return conn;
            }
        }
        return null;
    }

    public void readTerminal() {
        try(Scanner scanner = new Scanner(System.in)) {
            while(running) {
                String command = scanner.nextLine();
                String[] splitCommand = command.split("\\s+");
                switch (splitCommand[0]) {
                    case "exit":
                        log.info("[Registry] Closing registry node...");
                        running = false;
                        break;
                    case "setup-overlay": // add thread pool size
                        if(splitCommand.length > 1){
                            initializeOverlay(splitCommand[1]);
                        }
                        break;
                    case "start":
                        int numRounds = 0;
                        if(splitCommand.length > 1) {
                            numRounds = Integer.parseInt(splitCommand[1]);
                            sendTaskInitiate(numRounds);
                        }
                        break;
                    case "print-connections":
                        printConnectionMap();
                        break;
                    default:
                        log.warning("Unknown terminal command...");
                        break;
                }
            }
        }
    }

    private void sendTaskInitiate(int numRounds){
        try {
            log.info("Sending task initate command to messaging nodes...");
            TaskInitiate ti = new TaskInitiate(Protocol.TASK_INITIATE, numRounds);
            for(TCPConnection conn : openConnections) {
                conn.sender.sendData(ti.getBytes());
            }
        } catch(IOException e) {
            log.warning("Exception while sending initiate task message to node..." + e.getStackTrace());
        }
    }

    private void initializeOverlay(String threads) {
        int numThreads = Integer.parseInt(threads);
        OverlayCreator oc = new OverlayCreator(new ArrayList<>(nodeToConnMap.keySet()));
        overlay = oc.buildRing();
        connectionMap = overlay;
        sendConnectionMap();
        sendThreads(numThreads);
        sendTotalNumConnections(openConnections.size());
    }

    private void sendTotalNumConnections(int totalNumNodes) {
        try {
            log.info("Sending total number of registered nodes to each node...");
            for(Map.Entry<NodeID, List<NodeID>> entry : connectionMap.entrySet()){
                NodeID node = entry.getKey();
                TCPConnection conn = nodeToConnMap.get(node);
                String nodes = Integer.toString(totalNumNodes);
                Message threadMessage = new Message(Protocol.TOTAL_NUM_NODES, (byte) 0, nodes);
                conn.sender.sendData(threadMessage.getBytes());
            }
        } catch(IOException e) {
            log.warning("Exception while sending total number of nodes to each node..." + e.getStackTrace());
        }
    }

    private void sendThreads(int numThreads) {
        try{
            log.info("Sending thread count to nodes...");
            for(Map.Entry<NodeID, List<NodeID>> entry : connectionMap.entrySet()){
                NodeID node = entry.getKey();
                TCPConnection conn = nodeToConnMap.get(node);
                String threads = Integer.toString(numThreads);
                Message threadMessage = new Message(Protocol.THREADS, (byte) 0, threads);
                conn.sender.sendData(threadMessage.getBytes());
            }
            
        }catch(IOException e) {
            log.warning("Exception while sending thread count to nodes..." + e);
        }
    }

    private void sendConnectionMap() {
        try {
            log.info("Sending connections to the messaging nodes...");
            for(Map.Entry<NodeID, List<NodeID>> entry : connectionMap.entrySet()) {
                NodeID node = entry.getKey();
                TCPConnection conn = nodeToConnMap.get(node);
                List<NodeID> peers = entry.getValue();
                int numConnections = peers.size();
                MessagingNodesList instructions = new MessagingNodesList(peers, numConnections);
                conn.sender.sendData(instructions.getBytes());
            }
        }catch(IOException e) {
            log.warning("Exception while sending connection map to compute nodes..." + e.getStackTrace());
        }
    }

    private void printConnectionMap() {
        for(Map.Entry<NodeID, List<NodeID>> entry : connectionMap.entrySet()){
            log.info(entry.toString());
        }
    }

    public static void main(String[] args) {
        LogConfig.init(Level.WARNING);
        Registry reg = new Registry(Integer.parseInt(args[0]));
        new Thread(reg::startRegistry).start();
        new Thread(reg::readTerminal).start();
    }
}
