package com.github.minyk.morphlinesmr.mapper;

import com.github.minyk.morphlinesmr.MorphlineMRConfig;
import com.github.minyk.morphlinesmr.partitioner.ExceptionPartitioner;
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
        URL file = MorphlineMapperTest.class.getClassLoader().getResource("morphline_with_exception.conf");
        mapDriver.getConfiguration().set(MorphlineMRConfig.MORPHLINE_FILE,file.getPath());
        mapDriver.getConfiguration().set(MorphlineMRConfig.MORPHLINE_ID, "morphline1");
        mapDriver.getConfiguration().set("exceptionkey", ExceptionPartitioner.EXCEPTION_KEY);
    }

    @Test
    public void testNormalCase() {
        mapDriver.clearInput();
        mapDriver.withInput(new LongWritable(0), new Text("Feb  4 10:46:14 syslog sshd[607]: listening on 0.0.0.0 port 22."));
        mapDriver.withOutput(new Text("syslog,sshd"), new Text("2943974000,syslog,sshd,listening on 0.0.0.0 port 22."));
        try {
            mapDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testExceptionCase() {
        mapDriver.clearInput();
        mapDriver.withInput(new LongWritable(0), new Text("<>Feb  4 10:46:14 syslog sshd[607]: listening on 0.0.0.0 port 22."));
        mapDriver.withOutput(new Text(ExceptionPartitioner.EXCEPTION_KEY), new Text("<>Feb  4 10:46:14 syslog sshd[607]: listening on 0.0.0.0 port 22."));
        try {
            mapDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}