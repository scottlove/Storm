package stormapp.Inputters;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Random;


public class ThreadInputter extends Thread{
    String [] sentences;
    Random rand;
    private SpoutOutputCollector collector;

    public ThreadInputter(SpoutOutputCollector c)
    {
        sentences = new String []{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
                "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
        rand = new Random();
        collector = c;

    }

    @Override
    public void run() {

        try
        {
        System.out.println("running ThreadInputter ***********************")    ;

        while(true )
        {
            String sentence = sentences[rand.nextInt(sentences.length)];
            collector.emit(new Values(sentence));
            Utils.sleep(100);
        }
        }
        catch(Exception e)
        {
            System.out.println(e.getMessage() + "*************************************")    ;
            return;
        }



    }
}
