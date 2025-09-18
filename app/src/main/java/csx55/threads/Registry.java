package csx55.threads;

import csx55.transport.TCPConnection;
import csx55.util.LogConfig;
import csx55.util.OverlayCreator;
import csx55.wireformats.Event;
import csx55.wireformats.Message;
import csx55.wireformats.NodeID;
import csx55.wireformats.Protocol;
import csx55.wireformats.Register;

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

    private Map<NodeID, TCPConnection> connections = new ConcurrentHashMap<>();

    public Registry(int port) {
        this.port = port;
    }
    
    @Override
    public void onEvent(Event event, Socket socket) {

        if(event.getType() == Protocol.REGISTER_REQUEST) {
            log.info("Register request detected. Checking status...");
            Register node = (Register) event;
            if (connections.containsKey(node.nodeID)) {
                sendRegisterSuccess(node);
            }
            else {
                sendRegisterFailure(node);
            }
        }
        
    }

    private void sendRegisterSuccess(Register node) {
        try {
            if(connections.containsKey(node.nodeID)) {
                log.info(() -> "New node was added to the registry successfully!\n" + "\tCurrent number of nodes in registry: " + connections.size());
                String info = "Registration request successful. The number of messaging nodes currently constituting the overlay is (" + connections.size() + ")";
                Message successMessage = new Message(Protocol.REGISTER_RESPONSE, (byte)0, info);
                log.info(() -> "Sending success response to messaging node at %s" + node.nodeID.toString());
                connections.get(node.nodeID).sender.sendData(successMessage.getBytes());
            }
        } catch(IOException e) {
            log.warning("Exception while registering compute node..." + e.getStackTrace());
        }
    }

    private void sendRegisterFailure(Register node) {
        try {
            if(!connections.containsKey(node.nodeID)) {
                log.info(() -> "Failure ocurred while registering node....");
                String info = "Registration request failed.";
                Message failureMessage = new Message(Protocol.REGISTER_RESPONSE, (byte)1, info);
                connections.get(node.nodeID).sender.sendData(failureMessage.getBytes());
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
                NodeID nodeID = new NodeID(client.getAddress().getHostAddress(), client.getPort());
                TCPConnection conn = new TCPConnection(clientSocket, this);
                conn.startReceiverThread();
                connections.put(nodeID, conn);
            }
        } catch(IOException e) {
            log.warning("Exception while accepting connections..." + e.getStackTrace());
        }
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
                    default:
                        log.warning("Unknown terminal command...");
                        break;
                }
            }
        }
    }

    public static void main(String[] args) {
        LogConfig.init(Level.INFO);
        Registry reg = new Registry(Integer.parseInt(args[0]));
        new Thread(reg::startRegistry).start();
        // new Thread(reg::readTerminal).start();
    }
}
