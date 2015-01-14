package com.github.minyk.morphlinesmr.partitioner;

import com.github.minyk.morphlinesmr.MorphlinesMRConfig;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by drake on 10/8/14.
 */
public class ExceptionPartitioner extends Partitioner<Text, Text> implements Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionPartitioner.class);
    public static final String EXCEPTION_KEY_VALUE ="__EXCEPTION__";
    private Configuration conf;
    private int exception_reducers;
    private HashPartitioner<Text, Text> hash;
    private Text partitionKey;

    @Override
    public int getPartition(Text key, Text value, int i) {
        if(key.toString().startsWith(EXCEPTION_KEY_VALUE)) {
            /**
             * In exception case,
             * 1. Use UTC milliseconds as a partition key OR
             * 2. Use string from morpline as a partition key
             */
            if(key.getLength() > EXCEPTION_KEY_VALUE.length()) {
                partitionKey.set(key.toString().substring(EXCEPTION_KEY_VALUE.length()-1, key.getLength()));
            } else {
                partitionKey.set(String.valueOf(new Date().getTime()));
            }
            int re = hash.getPartition(partitionKey, value, exception_reducers) + (i - exception_reducers);
            return re;
        } else {
            int re = hash.getPartition(key, value, i - exception_reducers);
            return re;
        }
    }

    @Override
    public void setConf(Configuration entries) {
        conf = entries;
        exception_reducers = conf.getInt(MorphlinesMRConfig.MORPHLINESMR_REDUCERS_EXCEPTION, 1);
        hash = new HashPartitioner<Text, Text>();
        partitionKey = new Text();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
