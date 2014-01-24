package stormapp.Inputters;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import junit.framework.TestCase;
import org.apache.commons.lang.ObjectUtils;
import stormapp.Topology.TopologyKafka;
import stormapp.spouts.KafkaSpout;

/**
 * Created by scotlov on 1/23/14.
 */
public class testKafka extends TestCase {




//
//    public void testKafkaSpout()
//    {
//        //Configuration
//        Config conf = new Config();
//        //conf.put("zookeeper","kafkaserver.cloudapp.net:2181");
//        conf.put("zookeeper","localhost:2181");
//        conf.put("groupID","3");
//        conf.put("kafkaThreads","1");
//        conf.put("topic","ptTest");
//
//        KafkaSpout k = new KafkaSpout();
//        k.startKafkaThread(conf);
//
//        Utils.sleep(20000);
//    }

    public void testKafkaTopology() throws Exception
    {

        String[] args = null;
        TopologyKafka.main(args);


        Utils.sleep(2000000);


    }
}
