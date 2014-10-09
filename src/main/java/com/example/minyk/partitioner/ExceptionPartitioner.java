package com.example.minyk.partitioner;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public int getPartition(Text text, Text value, int i) {
        if(text.toString().contains(EXCEPTION_KEY)) {
            if(LOGGER.isDebugEnabled()) {
                LOGGER.debug("Exception Record: " + value.toString());
            }
            return super.getPartition(text, value, exception_reducers) + i - exception_reducers;
        } else {
            if(LOGGER.isDebugEnabled()) {
                LOGGER.debug("Normal Record: " + value.toString());
            }
            return super.getPartition(text, value, i-exception_reducers);
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
