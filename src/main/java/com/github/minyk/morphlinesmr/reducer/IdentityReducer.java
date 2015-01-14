package com.github.minyk.morphlinesmr.reducer;

import com.github.minyk.morphlinesmr.counter.MorphlinesMRCounters;
import com.github.minyk.morphlinesmr.partitioner.ExceptionPartitioner;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by drake on 10/8/14.
 */
public class IdentityReducer extends Reducer<Text,Text,NullWritable,Text>{

    private static final Logger LOGGER = LoggerFactory.getLogger(IdentityReducer.class);

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("Reduce key: " + key.toString());
        }
        for(Text t : values) {
            context.getCounter(MorphlinesMRCounters.COUNTERGROUP, MorphlinesMRCounters.Reducer.COUNTER_INPUTTOTAL).increment(1L);
            context.write(NullWritable.get(),t);
        }
    }
}
