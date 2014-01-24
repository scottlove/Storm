package stormapp.DataOutputter;

import junit.framework.TestCase;


public class TCPMessageSenderTest extends TestCase {
    public void testSendMessage() throws Exception {

        TCPMessageSender s = new TCPMessageSender(8081,"localhost")   ;
        for (Integer i =0;i<10;i++)
        {

           //s.sendMessage("test" + i.toString());
        }


    }
}
