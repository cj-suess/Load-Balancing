package csx55.wireformats;

import java.util.logging.*;

public class EventFactory {

    private final Logger log = Logger.getLogger(this.getClass().getName());
    private final byte[] data;

    public EventFactory(byte[] data) {
        this.data = data;
    }

    public Event createEvent() {
        return null;
    }
    
}
