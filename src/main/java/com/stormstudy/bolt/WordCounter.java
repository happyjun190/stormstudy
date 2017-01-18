package com.stormstudy.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wushenjun on 17-1-18.
 */
public class WordCounter extends BaseBasicBolt {
    private HashMap<String, Integer> counters = new HashMap<String, Integer>();
    private volatile boolean edit = false;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        final long timeOffset = Long.parseLong(stormConf.get("TIME_OFFSET").toString());
        new Thread(new Runnable() {
            public void run() {
                while (true) {
                    if(edit) {
                        System.out.println("word counter printer start -----------------------------------");
                        for(String key:counters.keySet()) {
                            System.out.println(key+":"+counters.get(key));
                        }
                        System.out.println("word counter printer end -----------------------------------");
                    }
                    try {
                        Thread.sleep(timeOffset * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String str = tuple.getString(0);
        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
        edit = true;
        System.out.println("WordCounter+++++++++++++++++++++++++++++++++++++++++++");

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
