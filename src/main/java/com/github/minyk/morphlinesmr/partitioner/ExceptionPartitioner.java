package com.github.minyk.morphlinesmr.partitioner;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by drake on 10/8/14.
 */
public class ExceptionPartitioner extends HashPartitioner<Text, Text> implements Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionPartitioner.class);
    public static final String EXCEPTION_KEY="__EXCEPTION__";
    public static final String EXCEPRION_REDUCERS="morphline-mr.exception.reducers";
    private Configuration conf;
    private int exception_reducers;

    @Override
    public int getPartition(Text key, Text value, int i) {
        if(key.toString().contains(EXCEPTION_KEY)) {
            if(LOGGER.isDebugEnabled()) {
                LOGGER.debug("Exception Record: " + value.toString());
            }
            /**
             * In exception case,
             * 1. Use UTC milliseconds as a partition key OR
             * 2. Use string from morpline as a partition key
             */
            if(key.getLength() > EXCEPTION_KEY.length()) {
                key.set(key.toString().substring(EXCEPTION_KEY.length()-1, key.getLength()));
            } else {
                key.set(String.valueOf(new Date().getTime()));
            }
            return super.getPartition(key, value, exception_reducers) + i - exception_reducers;
        } else {
            if(LOGGER.isDebugEnabled()) {
                LOGGER.debug("Normal Record: " + value.toString());
            }
            return super.getPartition(key, value, i-exception_reducers);
        }
    }

    @Override
    public void setConf(Configuration entries) {
        conf = entries;
        exception_reducers = conf.getInt(EXCEPRION_REDUCERS, 1);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
