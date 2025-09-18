package csx55.threads;

import csx55.transport.TCPConnection;
import csx55.util.LogConfig;
import csx55.wireformats.Event;
import csx55.wireformats.NodeID;

import java.util.Map;
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

    public static void main(String[] args) {
        LogConfig.init(Level.INFO);
        Registry reg = new Registry(Integer.parseInt(args[0]));
        new Thread(reg::startRegistry).start();
        // new Thread(reg::readTerminal).start();
    }
}
