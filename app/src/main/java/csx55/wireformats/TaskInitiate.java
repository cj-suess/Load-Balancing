package csx55.wireformats;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class TaskInitiate implements Event {

    int messageType;
    public int numRounds;

    public TaskInitiate(int messageType, int numRounds) {
        this.messageType = messageType;
        this.numRounds = numRounds;
    }

    @Override
    public int getType() {
        return Protocol.TASK_INITIATE;
    }

    @Override
    public byte[] getBytes() throws IOException {
        byte[] encodedData = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(baos);
        dout.writeInt(messageType);
        /* FILL IN REQURED MARSHALING */
        dout.writeInt(numRounds);
        /*                           */
        dout.flush();
        encodedData = baos.toByteArray();
        baos.close();
        dout.close();
        return encodedData;
    }
    
}
