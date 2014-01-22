package stormapp.DataOutputter;

import junit.framework.TestCase;

/**
 * Created by scotlov on 1/17/14.
 */
public class TCPMessageSenderTest extends TestCase {
    public void testSendMessage() throws Exception {

        TCPMessageSender s = new TCPMessageSender(8081,"localhost")   ;
        for (Integer i =0;i<10;i++)
        {

            s.sendMessage("test" + i.toString());
        }


    }
}
