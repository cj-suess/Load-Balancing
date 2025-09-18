
package csx55.wireformats;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class EventFactoryTest {

    @Test
    public void testRegisterRequestEncodingDecoding() throws Exception {
        int messageType = Protocol.REGISTER_REQUEST;
        String ip = "127.0.0.1";
        int port = 5555;
        NodeID nodeID = new NodeID(ip, port);
        Register register = new Register(messageType, nodeID);
        byte[] bytes = register.getBytes();

        EventFactory factory = new EventFactory(bytes);
        Event event = factory.createEvent();
        assertNotNull(event);
        assertTrue(event instanceof Register);
        Register decoded = (Register) event;
        assertEquals(messageType, decoded.messageType);
        assertEquals(ip, decoded.nodeID.ip);
        assertEquals(port, decoded.nodeID.port);
    }
    
}
