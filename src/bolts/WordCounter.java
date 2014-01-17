package bolts;

import java.util.HashMap;
import java.util.Map;

import DataOutputter.TCPMessageSender;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WordCounter extends BaseBasicBolt {

    Integer id;
    String name;
    String hostname;
    int port;
    Map<String, Integer> counters;

    /**
     * At the end of the spout (when the cluster is shutdown
     * We will show the word counters
     *
     */


    @Override
    public void cleanup() {
        System.out.println("-- Word Counter ["+name+"-"+id+"] --");

        TCPMessageSender s = new TCPMessageSender(8081,"localhost")   ;
        for(Map.Entry<String, Integer> entry : counters.entrySet()){
            System.out.println(entry.getKey()+": "+entry.getValue());
            s.sendMessage(entry.getKey()+": "+entry.getValue());
        }
    }

    /**
     * On create
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.hostname = stormConf.get("AggHost").toString();
        this.port = Integer.parseInt(stormConf.get("AggPort").toString())  ;

        this.counters = new HashMap<String, Integer>();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();

    }

    public void OutputResults()
    {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
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
    }
}

