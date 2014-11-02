package com.example.minyk.reducer;

import com.example.minyk.counter.MorphlinesMRCounters;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by drake on 10/8/14.
 */
public class IdentityReducer extends Reducer<Text,Text,NullWritable,Text>{

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text t : values) {
            context.getCounter(MorphlinesMRCounters.COUNTERGROUP, MorphlinesMRCounters.Reducer.COUNTER_INPUTTOTAL).increment(1L);
            context.write(NullWritable.get(),t);
        }
    }
}
