package csx55.threads;

import java.net.Socket;

import csx55.util.LogConfig;
import csx55.wireformats.Event;
import java.util.logging.*;

public class Registry implements Node {

    private Logger log = Logger.getLogger(this.getClass().getName());
    
    @Override
    public void onEvent(Event event, Socket socket) {
        
    }

    public static void main(String[] args) {
        
        LogConfig.init(Level.INFO);
        
    }
}
