package csx55.wireformats;

import java.util.Objects;

public class NodeID {
    
    String ip;
    int port;

    public NodeID(String ip, int port){
        this.ip = ip;
        this.port = port;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof NodeID)) return false;
        NodeID nodeID = (NodeID) o;
        return port == nodeID.port && ip.equals(nodeID.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port);
    }

    @Override
    public String toString() {
        return ip + ":" + port;
    }
    
}
