package spouts;


import java.util.Map;

import Inputters.TestInputter;
import Inputters.ThreadInputter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Random;

public class WordReader2 extends BaseRichSpout {

    private SpoutOutputCollector collector;
    Random _rand;
    Thread t     ;


    public void ack(Object msgId) {
        System.out.println("OK:"+msgId);
    }
    public void close()
    {
        System.out.println("WordReader2 close**************************:");
        t.interrupt();


    }

    public void fail(Object msgId) {
        System.out.println("FAIL:"+msgId);
    }

    /**
     * The only thing that the methods will do It is emit each
     * file line
     */
    public void nextTuple() {
        //Do nothing, threadinputter is emitting
        Utils.sleep(100);

    }

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {


        this.collector = collector;
        _rand = new Random();
        t = new ThreadInputter(collector);
        t.start();

    }

    /**
     * Declare the output field "word"
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }
}
