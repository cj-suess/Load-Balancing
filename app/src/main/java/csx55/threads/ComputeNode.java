package csx55.threads;

import java.io.IOException;
import java.net.*;
import csx55.transport.TCPConnection;
import csx55.util.LogConfig;
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
    private NodeID node;

    private Map<NodeID, TCPConnection> connections = new ConcurrentHashMap<>();
    private Map<Socket, TCPConnection> socketToConn = new ConcurrentHashMap<>();
    private volatile List<NodeID> connectionList = List.of();


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
            connectionList = Collections.unmodifiableList(message.getPeers());
            connect();
        }
        else if(event.getType() == Protocol.THREADS){
            Message message = (Message) event;
            numThreads = Integer.parseInt(message.info);
            log.info("Recieving thread count from Registry...\n" + "\tThread Count :" + numThreads);
            // create TaskProcessor instance
            // notify other nodes of tasks created
        }
        else if(event.getType() == Protocol.TASK_INITIATE){
            TaskInitiate ti = (TaskInitiate) event;
            log.info("Received task initiate from Registry with " + ti.numRounds + " rounds...");
        }
    }

    private void register() {
        try{
            Socket socket = new Socket(registryNode.getIP(), registryNode.getPort());
            TCPConnection registryConn = new TCPConnection(socket, this);
            Register registerMessage = new Register(Protocol.REGISTER_REQUEST, node);
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

                Message idMessage = new Message(Protocol.NODE_ID, (byte)0, this.node.toString());
                conn.sender.sendData(idMessage.getBytes());
            } catch(IOException e) {
                log.warning("Exception while trying to connect to other compute nodes..." + e.getStackTrace());
            }
        }
    }

    private void startNode() {
        try {
            serverSocket = new ServerSocket(0);
            node = new NodeID(InetAddress.getLocalHost().getHostAddress(), serverSocket.getLocalPort());
            log = Logger.getLogger(ComputeNode.class.getName() + "[" + node.toString() + "]");
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

    public static void main(String[] args) {

        LogConfig.init(Level.INFO);
        ComputeNode node = new ComputeNode(args[0], Integer.parseInt(args[1]));
        new Thread(node::startNode, "Node-" + node.toString() + "-Server").start();
        new Thread(node::readTerminal, "Node-" + node.toString() + "-Terminal").start();
        
    }
    
}
