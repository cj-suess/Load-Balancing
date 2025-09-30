package csx55.wireformats;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

public class TaskRequest implements Event {
    
    public int messageType;
    public NodeID requesterId;
    public int numTasksRequested;
    public int ttl;
    public UUID uuid;

    public TaskRequest(int messageType, NodeID requesterId, int numTasksRequested, int numNodes, UUID uuid){
        this.messageType = messageType;
        this.requesterId = requesterId;
        this.numTasksRequested = numTasksRequested;
        this.ttl = numNodes;
        this.uuid = uuid; // CHECKPOINT -> use this to create unique requests from same requester. add to seenRequests check
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
        int newTtl = this.ttl - 1;
        dout.writeInt(newTtl);
        byte[] uuidBytes = uuid.toString().getBytes();
        int uuidLength = uuidBytes.length;
        dout.writeInt(uuidLength);
        dout.write(uuidBytes);
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
