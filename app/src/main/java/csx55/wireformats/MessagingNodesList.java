package csx55.wireformats;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class MessagingNodesList implements Event {

    public int numConnections;
    List<NodeID> peers;

    public MessagingNodesList(List<NodeID> peers, int numConnections) {
        this.numConnections = numConnections;
        this.peers = peers;
    }

    @Override
    public int getType() {
        return Protocol.MESSAGING_NODES_LIST;
    }

    @Override
    public byte[] getBytes() throws IOException {
        byte[] encodedData = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(baos);
        dout.writeInt(Protocol.MESSAGING_NODES_LIST);
        /* FILL IN REQURED MARSHALING */
        dout.writeInt(numConnections);
        writeNodes(dout, peers);
        /*                           */
        dout.flush();
        encodedData = baos.toByteArray();
        baos.close();
        dout.close();
        return encodedData;
    }

    private void writeNodes(DataOutputStream dout, List<NodeID> peers) throws IOException {
        for(NodeID node : peers) {
            byte[] ipBytes = node.ip.getBytes();
            int ipLength = ipBytes.length;
            dout.writeInt(ipLength);
            dout.write(ipBytes);
            dout.writeInt(node.getPort());
        }
    }

    public List<NodeID> getPeers() {
        return peers;
    }
    
}
