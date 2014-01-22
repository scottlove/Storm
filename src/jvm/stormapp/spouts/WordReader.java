package stormapp.spouts;


import java.util.Map;

import stormapp.Inputters.TestInputter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Random;

public class WordReader extends BaseRichSpout {

    private SpoutOutputCollector collector;
    Random _rand;
    TestInputter inputter;


    public void ack(Object msgId) {
        System.out.println("OK:"+msgId);
    }
    public void close() {}

    public void fail(Object msgId) {
        System.out.println("FAIL:"+msgId);
    }

    /**
     * The only thing that the methods will do It is emit each
     * file line
     */
    public void nextTuple() {
        Utils.sleep(100);
//        String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
//                "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
//        String sentence = sentences[_rand.nextInt(sentences.length)];
        if (inputter.iterator().hasNext())
        {   String sentence = (String)inputter.iterator().next();
            collector.emit(new Values(sentence));
        }


    }

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        inputter = new TestInputter();
        this.collector = collector;
        _rand = new Random();

    }

    /**
     * Declare the output field "word"
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }


}
