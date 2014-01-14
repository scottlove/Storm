package Inputters;


import java.util.Iterator;
import java.util.Random;


public class TestIterator implements Iterator {

    String [] sentences;
    Random rand;

    public TestIterator()
    {

       sentences = new String []{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
                "four score and seven years ago", "snow white and the seven dwarfs2", "i am at two with nature" };
        rand = new Random();

    }

    @Override
    public boolean hasNext() {

        return true;
    }

    @Override
    public String next() {
        return sentences[rand.nextInt(sentences.length)];

    }

    @Override
    public void remove() {

    }
}
