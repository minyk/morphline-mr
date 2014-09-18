package com.example.minyk.mapper;

import com.example.minyk.MorphlineMRDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
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

    MapDriver<LongWritable, Text, Text, Text> mapDriver;

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
        mapDriver.withInput(new LongWritable(0), new Text("<164>Feb  4 10:46:14 syslog sshd[607]: listening on 0.0.0.0 port 22."));
        mapDriver.withOutput(new Text("syslog,sshd"), new Text("1"));
        try {
            mapDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
