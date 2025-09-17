package csx55.threads;

import java.net.Socket;
import csx55.wireformats.Event;

public interface Node {

    void onEvent(Event event, Socket socket);
    
}
