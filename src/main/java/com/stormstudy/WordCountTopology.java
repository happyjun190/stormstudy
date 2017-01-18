package com.stormstudy;

import com.stormstudy.bolt.WordCounter;
import com.stormstudy.bolt.WordSpliter;
import com.stormstudy.spout.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by wushenjun on 17-1-17.
 * Word Count Topology
 */
public class WordCountTopology {
    public static void main(String[] args) {
        //提交topology时需要传入分析目录以及时间偏移量
        if(args.length!=2) {
            System.err.println("Usage: inputPath and timeOffset");
            System.err.println("such as : java -jar WordCountTopology.jar /opt/file 2\\");
        }

        //拓扑节点启动
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("word-reader", new WordReader());
        topologyBuilder.setBolt("word-spilter", new WordSpliter()).shuffleGrouping("word-reader");
        topologyBuilder.setBolt("word-counter", new WordCounter()).shuffleGrouping("word-spilter");
        String inputPath = args[0];
        String timeOffset = args[1];
        Config conf = new Config();
        conf.put("INPUT_PATH", inputPath);
        conf.put("TIME_OFFSET", timeOffset);
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCount", conf, topologyBuilder.createTopology());
    }
}
