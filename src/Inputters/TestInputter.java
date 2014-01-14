package Inputters;

import java.util.Iterator;

public class TestInputter implements Iterable{
    @Override
    public Iterator iterator() {
        return new TestIterator();
    }
}
