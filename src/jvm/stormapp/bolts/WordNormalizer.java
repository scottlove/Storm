package stormapp.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class WordNormalizer extends BaseRichBolt {

    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }
    public void cleanup() {}

    /**
     * The bolt will receive the line from the
     * words file and process it to Normalize this line
     *
     * The normalize will be put the words in lower case
     * and split the line to get all words in this
     */
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        System.out.println("Sentence is:" +sentence) ;
        String[] words = sentence.split(" ");
        for(String word : words){
            word = word.trim();
            System.out.println("word is:" + word) ;
            if(!word.isEmpty()){
                word = word.toLowerCase();
                _collector.emit(new Values(word));
            }
        }
    }


    /**
     * The bolt will only emit the field "word"
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}