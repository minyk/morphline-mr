package com.github.minyk.morphlinesmr;

import com.github.minyk.morphlinesmr.mapper.MorphlinesMapper;
import com.github.minyk.morphlinesmr.partitioner.ExceptionPartitioner;
import com.github.minyk.morphlinesmr.reducer.IdentityReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

/**
 * Created by drake on 10/10/14.
 */
public class MorphlineMRExceptionTest {
    MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> driver;

    @Before
    public void setUp() {
        MorphlinesMapper mapper = new MorphlinesMapper();
        IdentityReducer reducer = new IdentityReducer();
        ExceptionPartitioner partitioner = new ExceptionPartitioner();
        driver = new MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text>(mapper, reducer);

        URL file = MorphlineMRExceptionTest.class.getClassLoader().getResource("morphline_with_exception.conf");
        driver.getConfiguration().set(MorphlinesMRConfig.MORPHLINE_FILE,file.getPath());
        driver.getConfiguration().set(MorphlinesMRConfig.MORPHLINE_ID,"morphline1");
    }

    @Test
    public void testNormalCase() {
        driver.withInput(new LongWritable(0), new Text("Feb  4 10:46:14 syslog sshd[607]: listening on 0.0.0.0 port 22."));
        driver.withOutput(NullWritable.get(), new Text("2943974000,syslog,sshd,listening on 0.0.0.0 port 22."));
        try {
            driver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testExceptionCase() {
        driver.withInput(new LongWritable(0), new Text("<>Feb  4 10:46:14 syslog sshd[607]: listening on 0.0.0.0 port 22."));
        driver.withOutput(NullWritable.get(), new Text("<>Feb  4 10:46:14 syslog sshd[607]: listening on 0.0.0.0 port 22."));
        try {
            driver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
