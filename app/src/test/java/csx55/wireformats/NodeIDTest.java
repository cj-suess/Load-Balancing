package csx55.wireformats;

import org.junit.jupiter.api.Test;

public class NodeIDTest {

    @Test
    public void testNodeIDCreation() {
        String ip = "134:56:23:12";
        int port = 8080;
        NodeID nodeID = new NodeID(ip, port);
        assert nodeID.ip.equals(ip);
        assert nodeID.port == port;
    }
    
}
