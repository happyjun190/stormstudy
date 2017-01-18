package com.stormstudy.bolt;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by wushenjun on 17-1-18.
 */
public class WordSpliter extends BaseBasicBolt {
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String line = tuple.getString(0);
        String[] words = line.split(" ");
        for(String word:words) {
            word = word.trim();
            if(StringUtils.isNotBlank(word)) {
                word = word.toLowerCase();
                basicOutputCollector.emit(new Values(word));
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
