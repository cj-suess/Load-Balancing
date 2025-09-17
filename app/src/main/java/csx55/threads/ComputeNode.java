package csx55.threads;

import java.net.Socket;

import csx55.util.LogConfig;
import csx55.wireformats.Event;
import java.util.logging.Logger;
import java.util.logging.Level;

public class ComputeNode implements Node {

    private Logger log = Logger.getLogger(this.getClass().getName());

    @Override
    public void onEvent(Event event, Socket socket) {
    }

    public static void main(String[] args) {

        LogConfig.init(Level.INFO);
        
    }
    
}
