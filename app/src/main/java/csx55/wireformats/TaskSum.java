package csx55.wireformats;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class TaskSum implements Event {

    int messageType;
    public int taskSum;
    public NodeID nodeId;

    public TaskSum(int messageType, int taskSum, NodeID nodeId) {
        this.messageType = messageType;
        this.taskSum = taskSum;
        this.nodeId = nodeId;
    }

    @Override
    public int getType() {
        return Protocol.TASK_SUM;
    }

    @Override
    public byte[] getBytes() throws IOException {
        byte[] encodedData = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(baos);
        dout.writeInt(messageType);
        /* FILL IN REQURED MARSHALING */
        dout.writeInt(taskSum);
        byte[] ipBytes = nodeId.ip.getBytes();
        int ipLength = ipBytes.length;
        dout.writeInt(ipLength);
        dout.write(ipBytes);
        dout.writeInt(nodeId.port);
        /*                           */
        dout.flush();
        encodedData = baos.toByteArray();
        baos.close();
        dout.close();
        return encodedData;
    }
    
}
