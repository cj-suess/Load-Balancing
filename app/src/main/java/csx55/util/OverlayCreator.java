package csx55.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import csx55.transport.TCPConnection;
import csx55.wireformats.NodeID;

public class OverlayCreator {

    private Map<NodeID, TCPConnection> connections = new ConcurrentHashMap<>();
    private Map<NodeID, List<NodeID>> overlay = new ConcurrentHashMap<>();
    private List<NodeID> nodes;

    public OverlayCreator(){}

    public OverlayCreator(Map<NodeID, TCPConnection> connections){
        this.connections = connections;
        this.nodes = new ArrayList<>(connections.keySet());
    }

    public Map<NodeID, List<NodeID>> buildRing() {
        int n = connections.size();
        for(int i = 0; i < n; i++) {
            overlay.put(nodes.get(i), new ArrayList<>());
            overlay.get(nodes.get(i)).add(nodes.get((i+1) % connections.size()));
            overlay.get(nodes.get(i)).add(nodes.get((i-1+n) % n));
        }
        return overlay;
    }
    
}
