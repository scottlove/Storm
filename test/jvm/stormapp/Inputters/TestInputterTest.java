package stormapp.Inputters;

import junit.framework.TestCase;

import java.util.Iterator;


public class TestInputterTest extends TestCase {

    public void testIterator()
    {
        TestInputter t = new TestInputter();
        Iterator i = t.iterator();
        int count = 0;
        int maxCount = 100;

        while(i.hasNext() && count < maxCount)
        {
            System.out.println(i.next());
            count ++;
        }

        assertTrue(count == maxCount);

    }


}
