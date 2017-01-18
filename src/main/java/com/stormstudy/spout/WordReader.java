package com.stormstudy.spout;

import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.shade.org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by wushenjun on 17-1-18.
 * spout节点
 */
public class WordReader extends BaseRichSpout {

    private String inputPath;
    private SpoutOutputCollector collector;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        inputPath = (String) map.get("INPUT_PATH");
    }


    //循环读取文件信息
    public void nextTuple() {
        //读取除了以.bak文件为后缀的文件
        Collection<File> files = FileUtils.listFiles(new File(inputPath),
                FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".bak")), null);
        for(File file:files) {

            try {
                List<String> lines = FileUtils.readLines(file, "UTF-8");
                for(String line:lines) {
                    collector.emit(new Values(line));
                }
                FileUtils.moveFile(file, new File(file.getPath()+System.currentTimeMillis()+".bak"));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    //定义输出信息
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }
}
