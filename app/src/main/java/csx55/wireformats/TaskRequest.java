package csx55.wireformats;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class TaskRequest implements Event {
    
    public int messageType;
    public NodeID requesterId;
    public int numTasksRequested;
    public int ttl;

    public TaskRequest(int messageType, NodeID requesterId, int numTasksRequested, int numNodes){
        this.messageType = messageType;
        this.requesterId = requesterId;
        this.numTasksRequested = numTasksRequested;
        this.ttl = numNodes - 1;
    }

    @Override
    public int getType() {
        return Protocol.TASK_REQUEST;
    }

    @Override
    public byte[] getBytes() throws IOException {
        byte[] encodedData = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(baos);
        dout.writeInt(messageType);
        /* FILL IN REQURED MARSHALING */
        writeNodeID(dout);
        dout.writeInt(numTasksRequested);
        dout.writeInt(ttl);
        /*                           */
        dout.flush();
        encodedData = baos.toByteArray();
        baos.close();
        dout.close();
        return encodedData;
    }

    private void writeNodeID(DataOutputStream dout) throws IOException {
        byte[] ipBytes = requesterId.getIP().getBytes();
        int ipLength = ipBytes.length;
        dout.writeInt(ipLength);
        dout.write(ipBytes);
        dout.writeInt(requesterId.getPort());
    }

}
