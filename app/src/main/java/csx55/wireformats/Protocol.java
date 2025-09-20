package csx55.wireformats;

public interface Protocol {
    int REGISTER_REQUEST = 1;
    int REGISTER_RESPONSE = 2;
    int DEREGISTER_REQUEST = 3;
    int DEREGISTER_RESPONSE = 4;
    int NODE_ID = 5;
    int ID_MESSAGE = 6;
    int OVERLAY = 7;
    int MESSAGING_NODES_LIST = 8;
}
