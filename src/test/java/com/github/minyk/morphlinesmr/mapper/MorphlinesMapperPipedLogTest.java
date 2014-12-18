package com.github.minyk.morphlinesmr.mapper;

import com.github.minyk.morphlinesmr.MorphlinesMRConfig;
import com.github.minyk.morphlinesmr.partitioner.ExceptionPartitioner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

/**
 * Created by drake on 11/4/14.
 */
public class MorphlinesMapperPipedLogTest {
    private final String log = "991110020130115004900197|Brandon Lee|1916809917|LJKODFIJ|192.168.5.100|OIRIRJ|00:49:00 232||00:49:07 450||7218|125487|BJK22||";
    private final String exp_log = "o20130115779693||-910400528||192.168.5.110||00:01:29 286||00:01:29 410||124||EGH10||";

    MapDriver<LongWritable, Text, Text, Text> mapDriver;

    @Before
    public void setUp() {
        MorphlinesMapper mapper = new MorphlinesMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        URL file = MorphlinesMapperTest.class.getClassLoader().getResource("morphline_pipedlog.conf");
        mapDriver.getConfiguration().set(MorphlinesMRConfig.MORPHLINE_FILE,file.getPath());
        mapDriver.getConfiguration().set(MorphlinesMRConfig.MORPHLINE_ID, "morphline1");
        mapDriver.getConfiguration().set("exceptionkey", ExceptionPartitioner.EXCEPTION_KEY);
    }

    @Test
    public void testNormalCase() {
        mapDriver.clearInput();
        mapDriver.withInput(new LongWritable(0), new Text(log));
        mapDriver.withOutput(new Text("9911100"), new Text("9911100,20130115,004900,1916809917,192.168.5.100,00:49:00 232,00:49:07 450,7218,BJK22"));
        try {
            mapDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testExceptionCase() {
        mapDriver.clearInput();
        mapDriver.withInput(new LongWritable(0), new Text(exp_log));
        mapDriver.withOutput(new Text(ExceptionPartitioner.EXCEPTION_KEY), new Text("o20130115779693||-910400528||192.168.5.110||00:01:29 286||00:01:29 410||124||EGH10||"));
        try {
            mapDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
