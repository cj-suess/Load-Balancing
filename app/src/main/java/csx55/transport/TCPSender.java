package csx55.transport;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;

public class TCPSender {

    private Socket socket;
    private DataOutputStream dout;

    public TCPSender(Socket socket) throws IOException {
        this.socket = socket;
        dout = new DataOutputStream(socket.getOutputStream());
    }

    public synchronized void sendData(byte[] dataToSend) throws IOException {
         int dataLength = dataToSend.length;
         dout.writeInt(dataLength);
         dout.write(dataToSend, 0, dataLength);
         dout.flush();
    }
    
}

