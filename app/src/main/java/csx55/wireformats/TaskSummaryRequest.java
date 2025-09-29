package csx55.wireformats;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class TaskSummaryRequest implements Event {

    int messageType;

    public TaskSummaryRequest(int messageType) {
        this.messageType = messageType;
    }

    @Override
    public int getType() {
        return Protocol.PULL_TRAFFIC_SUMMARY;
    }

    @Override
    public byte[] getBytes() throws IOException {
        byte[] encodedData = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(baos);
        dout.writeInt(messageType);
        /* FILL IN REQURED MARSHALING */
        /*                           */
        dout.flush();
        encodedData = baos.toByteArray();
        baos.close();
        dout.close();
        return encodedData;
    }
    
}
