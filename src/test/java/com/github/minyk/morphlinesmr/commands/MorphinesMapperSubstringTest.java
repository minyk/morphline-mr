package com.github.minyk.morphlinesmr.commands;

import com.github.minyk.morphlinesmr.MorphlinesMRConfig;
import com.github.minyk.morphlinesmr.mapper.MorphlinesMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created by minyk on 15. 3. 22.
 */
public class MorphinesMapperSubstringTest {

    MapDriver<LongWritable, Text, Text, Text> mapDriver;

    @Before
    public void setUp() throws URISyntaxException {
        MorphlinesMapper mapper = new MorphlinesMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        URL file = MorphlinesMapperCSVTest.class.getClassLoader().getResource("morphline_substring.conf");
        mapDriver.addCacheFile(file.toURI());
        mapDriver.getConfiguration().set(MorphlinesMRConfig.MORPHLINE_FILE, file.getPath());
        mapDriver.getConfiguration().setBoolean(MorphlinesMRConfig.MORPHLINE_FILE_TEST, true);
        mapDriver.getConfiguration().set(MorphlinesMRConfig.MORPHLINE_ID, "morphline1");
    }

    @Test
    public void test() {
        mapDriver.clearInput();
        mapDriver.withInput(new LongWritable(0), new Text("01234,01234,01234"));
        mapDriver.withOutput(new Text("1"), new Text("01|234 "));
        try {
            mapDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
