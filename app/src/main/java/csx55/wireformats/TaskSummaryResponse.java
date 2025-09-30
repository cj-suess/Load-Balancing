package csx55.wireformats;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class TaskSummaryResponse implements Event {

    int messageType;
    public int serverPort;
    public int generatedTasks; 
    public int pulledTasks;
    public int pushedTasks;
    public int completedTasks;
    public float workloadPercentage;

    public TaskSummaryResponse(int messageType, int serverPort, int generatedTasks, int pulledTasks, int pushedTasks, int completedTasks, float workloadPercentage) {
        this.messageType = messageType;
        this.serverPort = serverPort;
        this.generatedTasks = generatedTasks;
        this.pulledTasks = pulledTasks;
        this.pushedTasks = pushedTasks;
        this.completedTasks = completedTasks;
        this.workloadPercentage = workloadPercentage;
    }

    @Override
    public int getType() {
        return Protocol.TRAFFIC_SUMMARY;
    }

    @Override
    public byte[] getBytes() throws IOException {
        byte[] encodedData = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(baos);
        
        dout.writeInt(messageType);
        dout.writeInt(serverPort);
        dout.writeInt(generatedTasks);
        dout.writeInt(pulledTasks);
        dout.writeInt(pushedTasks);
        dout.writeInt(completedTasks);
        dout.writeFloat(workloadPercentage);
        
        dout.flush();
        encodedData = baos.toByteArray();
        baos.close();
        dout.close();
        return encodedData;
    }
    
}
