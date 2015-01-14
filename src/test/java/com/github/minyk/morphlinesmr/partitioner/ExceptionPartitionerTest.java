package com.github.minyk.morphlinesmr.partitioner;

import com.github.minyk.morphlinesmr.MorphlinesMRConfig;
import junit.framework.Assert;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.minyk.morphlinesmr.MRJobConfig;


/**
 * Created by drake on 10/10/14.
 */
public class ExceptionPartitionerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionPartitionerTest.class);

    ExceptionPartitioner partitioner;
    MapReduceDriver driver;

    // This values are count from zero.
    private final int NORMAL_REDUCER_MAXIMUM = 7;
    private final int EXCEPTION_REDUCER_MAXIMUM = 1;

    @Before
    public void setUp() {
        driver = new MapReduceDriver();
        driver.getConfiguration().set(MorphlinesMRConfig.MORPHLINESMR_REDUCERS_EXCEPTION, String.valueOf(EXCEPTION_REDUCER_MAXIMUM + 1));
        driver.getConfiguration().set(MRJobConfig.NUM_REDUCES, String.valueOf(NORMAL_REDUCER_MAXIMUM + EXCEPTION_REDUCER_MAXIMUM + 2));
        partitioner = new ExceptionPartitioner();
        partitioner.setConf(driver.getConfiguration());
    }

    @Test
    public void testNormalCase() {
        int result = partitioner.getPartition(new Text("syslog,sshd"), new Text("1"), NORMAL_REDUCER_MAXIMUM + EXCEPTION_REDUCER_MAXIMUM + 2);
        LOGGER.info("Normal Case result: " + result);
        Assert.assertFalse(result > NORMAL_REDUCER_MAXIMUM);
    }

    @Test
    public void testExceptionCase() {
        int result = partitioner.getPartition(new Text(ExceptionPartitioner.EXCEPTION_KEY_VALUE), new Text("1"), NORMAL_REDUCER_MAXIMUM + EXCEPTION_REDUCER_MAXIMUM + 2);
        LOGGER.info("Exception Case result: " + result);
        Assert.assertFalse(result <= NORMAL_REDUCER_MAXIMUM);
    }

}