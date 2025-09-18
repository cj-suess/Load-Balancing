package csx55.threads;

import java.io.IOException;
import java.net.*;

import csx55.transport.TCPConnection;
import csx55.util.LogConfig;
import csx55.wireformats.Event;
import csx55.wireformats.Message;
import csx55.wireformats.NodeID;
import csx55.wireformats.Protocol;

import java.util.logging.Logger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class ComputeNode implements Node {

    private Logger log = Logger.getLogger(this.getClass().getName());
    private ServerSocket serverSocket;
    private boolean running = true;

    private NodeID registryNode;
    private NodeID nodeID;

    private Map<NodeID, TCPConnection> connections = new ConcurrentHashMap<>();
    private Map<Socket, TCPConnection> socketToConn = new ConcurrentHashMap<>();

    public ComputeNode(String host, int port) {
        registryNode = new NodeID(host, port);
    }

    @Override
    public void onEvent(Event event, Socket socket) {
        if(event == null) {
            log.warning("Null event received from Event Factory...");
        }
        else if(event.getType() == Protocol.REGISTER_RESPONSE) {
            Message message = (Message) event; 
            System.out.println(message.info);
        }
        else if(event.getType() == Protocol.NODE_ID){
            Message message = (Message) event;
            String remoteNodeID = message.info;
            TCPConnection conn = socketToConn.get(socket);
            openConnections.put(remoteNodeID, conn);
        }
    }

    private void startNode() {
        try {
            serverSocket = new ServerSocket(0);
            nodeID = new NodeID(InetAddress.getLocalHost().getHostAddress(), serverSocket.getLocalPort());
            log = Logger.getLogger(ComputeNode.class.getName() + "[" + nodeID.toString() + "]");
            log.info("Compute node is up and running.\n \tListening on port: " + nodeID.getPort() + "\n" + "\tIP Address: " + nodeID.getIP());
            //register();
            while(running) {
                Socket clientSocket = serverSocket.accept();
                log.info("New connection from: " + nodeID.toString());
                TCPConnection conn = new TCPConnection(clientSocket, this);
                conn.startReceiverThread();
                socketToConn.put(clientSocket, conn);
            }
        }catch(IOException e) {
            log.warning("Exception in startNode...." + e.getStackTrace());
        }
    }

    public static void main(String[] args) {

        LogConfig.init(Level.INFO);
        ComputeNode node = new ComputeNode(args[0], Integer.parseInt(args[1]));
        new Thread(node::startNode, "Node-" + node.nodeID.toString() + "-Server").start();
        // new Thread(node::readTerminal, "Node-" + node.nodeID + "-Terminal").start();
        
    }
    
}
