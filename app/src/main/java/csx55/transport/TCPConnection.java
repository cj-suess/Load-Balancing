package csx55.transport;

import java.io.IOException;
import java.net.Socket;
import csx55.threads.Node;

public class TCPConnection {

    public Socket socket;
    public TCPReceiverThread receiver;
    public TCPSender sender;
    public Node node;

    public TCPConnection(Socket socket, Node node) throws IOException {
        this.socket = socket;
        this.node = node;
        this.sender = new TCPSender(socket);
        this.receiver = new TCPReceiverThread(socket, node);
    }

    public void startReceiverThread() {
        new Thread(receiver).start();
    }

}

