package stormapp.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import backtype.storm.zookeeper__init;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import stormapp.Inputters.KafkaConsumer;


import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class KafkaSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private ExecutorService executor;
    Thread t     ;
    private ConsumerConnector consumer;


    public void ack(Object msgId) {
        System.out.println("OK:"+msgId);
    }
    public void close()
    {
        System.out.println("KafkaSpout close**************************:");
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
        //collector.emit(new Values("test"));
        Utils.sleep(100);

    }

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {



        String topic = conf.get("topic").toString();;
        String zookeeper = conf.get("zookeeper").toString();
        String groupID = conf.get("groupID").toString();
        int threads = Integer.parseInt(conf.get("kafkaThreads").toString());

        System.out.println();
        System.out.println();
        System.out.println("topic:" + topic);
        System.out.println("zookeeper:" + zookeeper);
        System.out.println("groupID:" + groupID);
        System.out.println("kafka threads:" + threads);
        System.out.println();
        System.out.println();

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupID));

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(threads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // now launch all the threads
        executor = Executors.newFixedThreadPool(threads);

        int threadNumber = 0;

        for (final KafkaStream stream : streams)
        {
            KafkaConsumer c = new KafkaConsumer(collector,stream, threadNumber)  ;
            executor.submit(c);
            threadNumber++;
        }

    }


    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId)
    {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }


    /**
     * Declare the output field "word"
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

}
