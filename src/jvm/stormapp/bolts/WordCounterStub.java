package stormapp.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import stormapp.DataOutputter.TCPMessageSender;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Scott on 1/27/14.
 */
public class WordCounterStub extends BaseRichBolt {


    Map<String, Integer> counters;

    int count;

    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;

        this.counters = new HashMap<String, Integer>();

        count =0;
    }

    private void postData()
    {

        System.out.println("posting data")             ;
        for(Map.Entry<String, Integer> entry : counters.entrySet()){
            System.out.println(entry.getKey()+": "+entry.getValue());
        }
        System.out.println("end posting data")        ;
    }

    @Override
    public void cleanup() {

    }




    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}


    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);
        /**
         * If the word dosn't exist in the map we will create
         * this, if not We will add 1
         */

        System.out.println(str)   ;
        if(!counters.containsKey(str)){
            counters.put(str, 1);
        }else{
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }

        count ++;
        if (count > 5)
        {
            count = 0;
            postData();
        }
    }
}
