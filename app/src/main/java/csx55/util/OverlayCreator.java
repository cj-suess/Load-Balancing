package csx55.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import csx55.transport.TCPConnection;
import csx55.wireformats.NodeID;

public class OverlayCreator {

    private List<NodeID> nodes;
    private Map<NodeID, List<NodeID>> overlay = new ConcurrentHashMap<>();
    private Map<NodeID, List<NodeID>> connectionMap = new ConcurrentHashMap<>();

    public OverlayCreator(){}

    public OverlayCreator(List<NodeID> nodes){
        this.nodes = nodes;
    }

    public Map<NodeID, List<NodeID>> buildRing() {
        int n = nodes.size();
        for(int i = 0; i < n; i++) {
            overlay.put(nodes.get(i), new ArrayList<>());
            overlay.get(nodes.get(i)).add(nodes.get((i+1) % n));
            overlay.get(nodes.get(i)).add(nodes.get((i-1+n) % n));
        }
        return overlay;
    }

    public Map<NodeID, List<NodeID>> filter(Map<NodeID, List<NodeID>> overlay) {
        for(Map.Entry<NodeID, List<NodeID>> entry : overlay.entrySet()){
            List<NodeID> filtered = new ArrayList<>();
            for(NodeID id : entry.getValue()){
                if(entry.getKey().compareTo(id) < 0){
                    filtered.add(id);
                }
            }
            connectionMap.put(entry.getKey(), filtered);
        }
        return connectionMap;
    }
    
}
