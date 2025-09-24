package csx55.wireformats;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.*;
import csx55.hashing.Task;

public class TaskExcess implements Event {

    private Logger log = Logger.getLogger(this.getClass().getName());

    int messageType;
    BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();

    @Override
    public int getType() {
        return Protocol.TASK_EXCESS;
    }

    @Override
    public byte[] getBytes() throws IOException {
        byte[] encodedData = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(baos);
        dout.writeInt(messageType);
        /* FILL IN REQURED MARSHALING */
        writeQueue(dout, taskQueue);
        /*                           */
        dout.flush();
        encodedData = baos.toByteArray();
        baos.close();
        dout.close();
        return encodedData;
    }

    private void writeQueue(DataOutputStream dout, BlockingQueue<Task> taskQueue) {
        for(Task t : taskQueue){
            try {
                writeTask(dout, t);
            } catch(IOException e) {
                log.warning("Exception while writing task queue in excess message..." + e.getStackTrace());
            }
        }
    }

    private void writeTask(DataOutputStream dout, Task task) throws IOException{
        writeString(dout, task.getIp());
        dout.writeInt(task.getPort());
        dout.writeInt(task.getRoundNumber());
        dout.writeInt(task.getPayload());
        dout.writeLong(task.getTimestamp());
        dout.writeLong(Thread.currentThread().getId());
        dout.writeInt(task.getNonce());
    }

    private void writeString(DataOutputStream dout, String ip) throws IOException {
        byte[] ipBytes = ip.getBytes();
        int ipLength = ipBytes.length;
        dout.writeInt(ipLength);
        dout.write(ipBytes);
    }
    
}
