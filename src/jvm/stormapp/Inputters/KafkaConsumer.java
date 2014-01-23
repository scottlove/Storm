package stormapp.Inputters;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.util.Random;


public class KafkaConsumer implements Runnable{
    private KafkaStream m_stream;

    private int m_threadNumber;
    private SpoutOutputCollector collector;

    public KafkaConsumer(SpoutOutputCollector c,KafkaStream a_stream, int a_threadNumber)
    {
        m_threadNumber = a_threadNumber;

        m_stream = a_stream;
        collector = c;

    }

    @Override
    public void run() {

        try
        {
        System.out.println("running ThreadInputter ***********************")    ;

            @SuppressWarnings("unchecked")
            ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
            collector.emit(new Values("Started ThreadInputter"));
            while (it.hasNext())
            {
                collector.emit(new Values(new String(it.next().message())));
                System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
            }

            collector.emit(new Values("Left ThreadInputter"));
        }
        catch(Exception e)
        {
            System.out.println(e.getMessage() + "*************************************")    ;
            return;
        }



    }
}
