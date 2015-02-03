package com.github.minyk.morphlinesmr.mapper;

import com.github.minyk.morphlinesmr.MorphlinesMRConfig;
import com.github.minyk.morphlinesmr.partitioner.ExceptionPartitioner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;


/**
 * Created by drake on 9/16/14.
 */
public class MorphlinesMapperCSVTest {

    MapDriver<LongWritable, Text, Text, Text> mapDriver;

    @Before
    public void setUp() throws URISyntaxException {
        MorphlinesMapper mapper = new MorphlinesMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        URL file = MorphlinesMapperCSVTest.class.getClassLoader().getResource("morphline_writecsv.conf");
        mapDriver.addCacheFile(file.toURI());
        mapDriver.getConfiguration().set(MorphlinesMRConfig.MORPHLINE_FILE,file.getPath());
        mapDriver.getConfiguration().set(MorphlinesMRConfig.MORPHLINE_ID, "morphline1");
    }

    @Test
    public void testNormalCase() {
        mapDriver.clearInput();
        mapDriver.withInput(new LongWritable(0), new Text("1,2,3"));
        mapDriver.withOutput(new Text("1"), new Text("3|2|1"));
        try {
            mapDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
