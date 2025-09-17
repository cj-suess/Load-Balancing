package csx55.wireformats;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Register implements Event, Protocol {

    public int messageType;
    public String ip;
    public int port;

    public Register(int messageType, String ip, int port) {
        this.messageType = messageType;
        this.ip = ip;
        this.port = port;
    }

    @Override
    public byte[] getBytes() throws IOException{
        byte[] encodedData = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(baos);
        dout.writeInt(messageType);
        /* FILL IN REQURED MARSHALING */
        byte[] ipBytes = ip.getBytes();
        int ipLength = ipBytes.length;
        dout.writeInt(ipLength);
        dout.write(ipBytes);
        dout.writeInt(port);
        /*                           */
        dout.flush();
        encodedData = baos.toByteArray();
        baos.close();
        dout.close();
        return encodedData;
    }

    @Override
    public int getType() {
        return Protocol.REGISTER_REQUEST;
    }
}
