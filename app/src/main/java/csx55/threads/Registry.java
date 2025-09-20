package csx55.threads;

import csx55.transport.TCPConnection;
import csx55.transport.TCPSender;
import csx55.util.LogConfig;
import csx55.util.OverlayCreator;
import csx55.wireformats.Event;
import csx55.wireformats.Message;
import csx55.wireformats.MessagingNodesList;
import csx55.wireformats.NodeID;
import csx55.wireformats.Overlay;
import csx55.wireformats.Protocol;
import csx55.wireformats.Register;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.*;
import java.io.IOException;
import java.net.*;

public class Registry implements Node {

    private Logger log = Logger.getLogger(this.getClass().getName());
    private int port;
    private ServerSocket serverSocket;
    private boolean running = true;

    List<TCPConnection> openConnections = new ArrayList<>();
    private Map<NodeID, TCPConnection> nodeToConnMap = new ConcurrentHashMap<>();
    private Map<NodeID, List<NodeID>> overlay = new ConcurrentHashMap<>();
    private Map<NodeID, List<NodeID>> connectionMap = new ConcurrentHashMap<>();

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
                    case "setup-overlay":
                        OverlayCreator oc = new OverlayCreator(new ArrayList<>(nodeToConnMap.keySet()));
                        overlay = oc.buildRing();
                        connectionMap = oc.filter(overlay);
                        sendConnectionMap();
                        break;
                    default:
                        log.warning("Unknown terminal command...");
                        break;
                }
            }
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

    public static void main(String[] args) {
        LogConfig.init(Level.INFO);
        Registry reg = new Registry(Integer.parseInt(args[0]));
        new Thread(reg::startRegistry).start();
        new Thread(reg::readTerminal).start();
    }
}
