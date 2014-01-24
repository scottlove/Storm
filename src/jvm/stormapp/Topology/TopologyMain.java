package stormapp.Topology;


import backtype.storm.StormSubmitter;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import stormapp.bolts.WordCounter;
import stormapp.bolts.WordNormalizer;
import stormapp.spouts.WordReader2;


public class TopologyMain {
    public static void main(String[] args) throws Exception {


        try
        {


        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader",new WordReader2());
        builder.setBolt("word-normalizer", new WordNormalizer())
                .shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(),1)
                .fieldsGrouping("word-normalizer", new Fields("word"));


        //Configuration
        Config conf = new Config();


        conf.setDebug(true);


        if (args != null && args.length > 0) {

            if(args.length !=3)
            {
                System.out.println("production mode requires 3 params:");
                System.out.println("topology aggHost aggPort");
            }
            else
            {
                conf.setNumWorkers(3);
                String topology = args[0];
                String aggHost = args[1];
                String aggPort = args[2];
                System.out.println("topology:" + topology);
                System.out.println("aggHost:" + aggHost);
                System.out.println("aggPort:" + aggPort);

                conf.put("AggHost", aggHost)    ;
                conf.put("AggPort",aggPort)    ;




                conf.setNumWorkers(3);


                StormSubmitter.submitTopology(topology, conf, builder.createTopology());
            }
        }
        else {
            conf.setMaxTaskParallelism(3);
            conf.put("AggHost", "localhost")    ;
            conf.put("AggPort","8081")    ;



            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());


            Thread.sleep(200000);


            cluster.shutdown();
        }
        }
        catch (Exception e)
        {
            System.out.println(e.toString());
        }



    }
}