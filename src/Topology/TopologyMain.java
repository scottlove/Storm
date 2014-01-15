package Topology;


import backtype.storm.StormSubmitter;
import spouts.WordReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.WordCounter;
import bolts.WordNormalizer;
import spouts.WordReader2;


public class TopologyMain {
    public static void main(String[] args) throws Exception {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader",new WordReader2());
        builder.setBolt("word-normalizer", new WordNormalizer())
                .shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(),1)
                .fieldsGrouping("word-normalizer", new Fields("word"));


        //Configuration
        Config conf = new Config();

        conf.setDebug(false);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);


            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);


            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());


            Thread.sleep(10000);


            cluster.shutdown();
        }



    }
}