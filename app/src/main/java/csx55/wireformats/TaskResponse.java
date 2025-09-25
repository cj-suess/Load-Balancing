package csx55.wireformats;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import csx55.hashing.Task;

public class TaskResponse implements Event {
    
    public int messageType;
    public NodeID requesterId;
    public List<Task> tasks;

    public TaskResponse(int messageType, NodeID requesterId, List<Task> tasks){
        this.messageType = messageType;
        this.requesterId = requesterId;
        this.tasks = tasks;
    }

    @Override
    public int getType() {
        return Protocol.TASK_RESPONSE;
    }

    @Override
    public byte[] getBytes() throws IOException {
        byte[] encodedData = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(baos);
        dout.writeInt(messageType);
        /* FILL IN REQURED MARSHALING */
        writeNodeID(dout);
        dout.writeInt(tasks.size());
        writeTaskList(dout);
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

    private void writeTaskList(DataOutputStream dout) throws IOException {
        for(Task task : tasks) {
            writeTask(dout, task);
        }
    }

    private void writeTask(DataOutputStream dout, Task task) throws IOException {
        byte[] ipBytes = task.getIp().getBytes();
        dout.writeInt(ipBytes.length);
        dout.write(ipBytes);
        dout.writeInt(task.getPort());
        dout.writeInt(task.getRoundNumber());
        dout.writeInt(task.getPayload());
    }

}
