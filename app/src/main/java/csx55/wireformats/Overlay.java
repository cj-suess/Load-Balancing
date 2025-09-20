package csx55.wireformats;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Overlay implements Event {
    
    public int messageType;
    public int numNodes;
    public int numConnections;
    public Map<NodeID, List<NodeID>> overlay;

    public Overlay(int messageType, int numNodes, int numConnections, Map<NodeID, List<NodeID>> overlay) {
        this.messageType = messageType;
        this.numNodes = numNodes;
        this.numConnections = numConnections;
        this.overlay = overlay;
    }

    @Override
    public int getType() {
        return Protocol.OVERLAY;
    }

    @Override
    public byte[] getBytes() throws IOException {
        byte[] encodedData = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(baos);
        dout.writeInt(messageType);
        /* FILL IN REQURED MARSHALING */
        dout.writeInt(numNodes);
        dout.writeInt(numConnections);
        writeMappings(dout, overlay);
        /*                           */
        dout.flush();
        encodedData = baos.toByteArray();
        baos.close();
        dout.close();
        return encodedData;
    }

    // method to write each String -> List mapping
    private void writeMappings(DataOutputStream dout, Map<NodeID, List<NodeID>> overlay) throws IOException {
        for(Map.Entry<NodeID, List<NodeID>> entry : overlay.entrySet()) {
            byte[] ipBytes = entry.getKey().getIP().getBytes();
            int ipLength = ipBytes.length;
            dout.writeInt(ipLength);
            dout.write(ipBytes);
            dout.writeInt(entry.getKey().getPort());
            for(NodeID node : entry.getValue()) {
                ipBytes = node.getIP().getBytes();
                ipLength = ipBytes.length;
                dout.writeInt(ipLength);
                dout.write(ipBytes);
                dout.writeInt(node.getPort());
            }
        }
    }
}

