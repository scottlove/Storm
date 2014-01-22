package stormapp.bolts;

import java.util.HashMap;
import java.util.Map;

import stormapp.DataOutputter.TCPMessageSender;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WordCounter extends BaseRichBolt {

    Integer id;
    String name;
    String hostname;
    int port;
    Map<String, Integer> counters;

    int count;

    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;

        this.counters = new HashMap<String, Integer>();
        this.hostname = conf.get("AggHost").toString();
        this.port = Integer.parseInt(conf.get("AggPort").toString())  ;
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
        count =0;
    }

    private void postData()
    {

        TCPMessageSender s = new TCPMessageSender(port,hostname)   ;
        for(Map.Entry<String, Integer> entry : counters.entrySet()){
            System.out.println(entry.getKey()+": "+entry.getValue());
            s.sendMessage(entry.getKey()+": "+entry.getValue());
        }
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
        if(!counters.containsKey(str)){
            counters.put(str, 1);
        }else{
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }

        count ++;
        if (count > 100)
        {
            count = 0;
            postData();
        }
    }
}

