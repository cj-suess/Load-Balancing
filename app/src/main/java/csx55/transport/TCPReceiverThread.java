package csx55.transport;

import csx55.threads.Node;
import csx55.wireformats.*;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.*;

public class TCPReceiverThread implements Runnable {

    private Socket socket;
    private DataInputStream din;
    private Node node;

    private Logger LOG = Logger.getLogger(TCPReceiverThread.class.getName());

    public TCPReceiverThread(Socket socket, Node node) throws IOException {
        this.socket = socket;
        this.node = node;
        din = new DataInputStream(socket.getInputStream());
    }

    public void run() {
        int dataLength;
        while(socket != null) {
            try {
                dataLength = din.readInt();
                byte[] data = new byte[dataLength];
                din.readFully(data, 0, dataLength);
                // pass data to EventFactory
                EventFactory ef = new EventFactory(data);
                Event decodedEvent = ef.createEvent();
                node.onEvent(decodedEvent, socket);
            } catch(SocketException soe) {
                LOG.warning("Socket exception caught reading data..." + soe.getLocalizedMessage());
                break;
            } catch(IOException ioe) {
                LOG.warning("IO exception caught reading data..." + ioe.getLocalizedMessage());
                break;
            } 
        }

    }
    
}

