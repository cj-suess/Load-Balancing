package csx55.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import csx55.wireformats.NodeID;
import java.util.logging.*;

public class OverlayCreator {

    private final static Logger log = Logger.getLogger(OverlayCreator.class.getName());

    private List<NodeID> nodes;
    private Map<NodeID, List<NodeID>> overlay = new ConcurrentHashMap<>();

    public OverlayCreator(){}

    public OverlayCreator(List<NodeID> nodes){
        this.nodes = nodes;
    }

    public Map<NodeID, List<NodeID>> buildRing() {
        log.info("Building overlay...");
        int n = nodes.size();
        for(int i = 0; i < n; i++) {
            overlay.put(nodes.get(i), new ArrayList<>());
            overlay.get(nodes.get(i)).add(nodes.get((i+1) % n));
        }
        return overlay;
    }
    
}
