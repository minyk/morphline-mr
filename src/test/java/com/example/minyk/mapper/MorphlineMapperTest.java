package com.example.minyk.mapper;

import com.example.minyk.MorphlineMRDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;


/**
 * Created by drake on 9/16/14.
 */
public class MorphlineMapperTest {

    MapDriver<LongWritable, Text, Text, NullWritable> mapDriver;

    @Before
    public void setUp() {
        MorphlineMapper mapper = new MorphlineMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        URL file = MorphlineMapperTest.class.getClassLoader().getResource("morphline.conf");
        Configuration conf = new Configuration();
        conf.set(MorphlineMRDriver.MORPHLINE_FILE,file.getPath());
        conf.set(MorphlineMRDriver.MORPHLINE_ID,"morphline1");
        mapDriver.setConfiguration(conf);
    }

    @Test
    public void testMapper() {
        mapDriver.withInput(new LongWritable(1), new Text("Mar 12 12:27:00 server3 named[32172]: lame server resolving 'jakarta5.wasantara.net.id' (in 'wasantara.net.id'?): 202.159.65.171#53"));
        mapDriver.withOutput(new Text("server3"), NullWritable.get());
        try {
            mapDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
