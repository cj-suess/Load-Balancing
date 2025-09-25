package csx55.wireformats;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Register implements Event, Protocol {

    public int messageType;
    public NodeID nodeID;

    public Register(int messageType, NodeID nodeID) {
        this.messageType = messageType;
        this.nodeID = nodeID;
    }

    @Override
    public byte[] getBytes() throws IOException{
        byte[] encodedData = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(baos);
        dout.writeInt(messageType);
        /* FILL IN REQURED MARSHALING */
        byte[] ipBytes = nodeID.getIP().getBytes();
        int ipLength = ipBytes.length;
        dout.writeInt(ipLength);
        dout.write(ipBytes);
        dout.writeInt(nodeID.getPort());
        /*                           */
        dout.flush();
        encodedData = baos.toByteArray();
        baos.close();
        dout.close();
        return encodedData;
    }

    @Override
    public int getType() {
        return messageType;
    }
}
