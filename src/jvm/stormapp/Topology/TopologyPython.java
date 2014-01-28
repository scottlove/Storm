package stormapp.Topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import stormapp.bolts.WordCounter;
import stormapp.bolts.WordCounterStub;
import stormapp.bolts.WordNormalizer;
import stormapp.spouts.WordReader2;

import java.util.Map;

/**
 * Created by Scott on 1/27/14.
 */
public class TopologyPython {

    public static class SplitSentence extends ShellBolt implements IRichBolt {

        public SplitSentence() {
            super("python", "splitsentence.py");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static void main(String[] args) throws Exception {


        try
        {


            //Topology definition
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("word-reader",new WordReader2());
            builder.setBolt("split", new SplitSentence())
                    .shuffleGrouping("word-reader");
            builder.setBolt("word-counter", new WordCounterStub(),1)
                    .fieldsGrouping("split", new Fields("word"));


            //Configuration
            Config conf = new Config();


            conf.setDebug(false);


            if (args != null && args.length > 0) {

                if(args.length !=3)
                {
                    System.out.println("production mode requires 1 params:");
                    System.out.println("topology ");
                }
                else
                {
                    conf.setNumWorkers(3);
                    String topology = args[0];

                    System.out.println("topology:" + topology);

                    conf.setNumWorkers(3);


                    StormSubmitter.submitTopology(topology, conf, builder.createTopology());
                }
            }
            else {
                conf.setMaxTaskParallelism(3);




                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("word-count", conf, builder.createTopology());


                Thread.sleep(20000);


                cluster.shutdown();
            }
        }
        catch (Exception e)
        {
            System.out.println(e.toString());
        }



    }
}
