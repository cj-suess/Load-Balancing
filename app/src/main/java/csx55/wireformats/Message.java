package csx55.wireformats;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Message implements Event {
    
    public int messageType;
    public byte statusCode;
    public String info;

    public Message(int messageType, byte statusCode, String info) {
        this.messageType = messageType;
        this.statusCode = statusCode;
        this.info = info;
    }

    @Override
    public byte[] getBytes() throws IOException {
        byte[] encodedData = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(baos);
        
        dout.writeInt(messageType);
        dout.writeByte(statusCode);
        byte[] infoBytes = info.getBytes();
        int infoLength = infoBytes.length;
        dout.writeInt(infoLength);
        dout.write(infoBytes);

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
