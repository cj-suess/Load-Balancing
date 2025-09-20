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

            log.info("Received new event...");
            int messageType = dis.readInt();

            switch (messageType) {
                case Protocol.REGISTER_REQUEST:
                    log.info("\tDecoding data into Register object.");
                    return readRegisterRequest(messageType, dis);
                case Protocol.REGISTER_RESPONSE:
                    log.info("\tDecoding data into Message object.");
                    return readStatusMessage(messageType, dis);
                case Protocol.NODE_ID:
                    log.info("\tDecoding data into Message object.");
                    return readStatusMessage(messageType, dis);
                case Protocol.OVERLAY:
                    log.info("\tDecoding data into Overlay object.");
                    return readOverlay(messageType, dis);
                case Protocol.MESSAGING_NODES_LIST:
                    log.info("\tDecoding data into MessagingNodesList object.");
                    return readMessagingNodesList(messageType, dis);
                default:
                    break;
            }

        } catch(IOException e) {
            log.warning("Exception while creating event..." + e.getMessage());
        }
        return null;
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
