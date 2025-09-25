package csx55.wireformats;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.*;

public class EventFactory {

    private final static Logger log = Logger.getLogger(EventFactory.class.getName());
    private final byte[] data;

    public EventFactory(byte[] data) {
        this.data = data;
    }

    public Event createEvent() {
        try(ByteArrayInputStream bais = new ByteArrayInputStream(data); DataInputStream dis = new DataInputStream(bais)) {

            //log.info("Received new event...");
            int messageType = dis.readInt();

            switch (messageType) {
                case Protocol.REGISTER_REQUEST:
                case Protocol.READY:
                    return readRegisterRequest(messageType, dis);
                case Protocol.REGISTER_RESPONSE:
                case Protocol.NODE_ID:
                case Protocol.THREADS:
                case Protocol.TOTAL_NUM_NODES:
                    return readStatusMessage(messageType, dis);
                case Protocol.OVERLAY:
                    return readOverlay(messageType, dis);
                case Protocol.MESSAGING_NODES_LIST:
                    return readMessagingNodesList(messageType, dis);
                case Protocol.TASK_INITIATE:
                    int numRounds = dis.readInt();
                    return new TaskInitiate(messageType, numRounds);
                case Protocol.TASK_SUM:
                    return readTaskSum(messageType, dis);
                case Protocol.TASK_REQUEST:
                    return readTaskRequest(messageType, dis);
                default:
                    break;
            }

        } catch(IOException e) {
            log.warning("Exception while creating event..." + e.getMessage());
        }
        return null;
    }

    private static TaskRequest readTaskRequest(int messageType, DataInputStream dis) throws IOException {
        String ip = readString(dis);
        int port = dis.readInt();
        int numTasksRequested = dis.readInt();
        return new TaskRequest(messageType, new NodeID(ip, port), numTasksRequested);
    }

    private static TaskSum readTaskSum(int messageType, DataInputStream dis) throws IOException {
        int taskSum = dis.readInt();
        String ip = readString(dis);
        int port = dis.readInt();
        NodeID nodeId = new NodeID(ip, port);
        return new TaskSum(messageType, taskSum, nodeId);
    }

    private static MessagingNodesList readMessagingNodesList(int messageType, DataInputStream dis) throws IOException {
        int numConnections = dis.readInt();
        List<NodeID> peers = readPeers(dis, numConnections);
        MessagingNodesList nodeListMessage = new MessagingNodesList(peers, numConnections);
        return nodeListMessage;
    }

    private static Overlay readOverlay(int messageType, DataInputStream dis) throws IOException {
        Map<NodeID, List<NodeID>> overlay = new HashMap<>();
        int numNodes = dis.readInt();
        int numConnections = dis.readInt();
        for(int i = 0; i < numNodes; i++) {
            String ip = readString(dis);
            int port = dis.readInt();
            NodeID nodeID = new NodeID(ip, port);
            overlay.put(nodeID, readPeers(dis, numConnections));
        }
        Overlay overlayMessage = new Overlay(messageType, numNodes, numConnections, overlay);
        return overlayMessage;
    }

    private static List<NodeID> readPeers(DataInputStream dis, int numConnections) throws IOException {
        List<NodeID> peers = new ArrayList<>();
        for(int i = 0; i < numConnections; i++) {
            peers.add(createPeer(dis));
        }
        return peers;
    }

    private static NodeID createPeer(DataInputStream dis) throws IOException {
        String ip = readString(dis);
        int port = dis.readInt();
        return new NodeID(ip, port);
    }

    private static Register readRegisterRequest(int messageType, DataInputStream dis) {
        try {
            String ip = readString(dis);
            int port = dis.readInt();
            NodeID nodeID = new NodeID(ip, port);
            Register register_request = new Register(messageType, nodeID);
            return register_request;
        } catch(IOException e) {
            log.warning("Exception while decoding register request...." + e.getMessage());
        }
        log.warning("Returning null instead of Register_Request object...");
        return null;
    }

    private static String readString(DataInputStream dis) throws IOException {
        int length = dis.readInt();
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        return new String(bytes);
    }

    private static Message readStatusMessage(int messageType, DataInputStream dis) throws IOException {
        byte statusCode = dis.readByte();
        String info = readString(dis);
        return new Message(messageType, statusCode, info);
    }
    
}
