package com.example.minyk.reducer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.example.minyk.MorphlineMRDriver;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by drake on 9/17/14.
 */
public class MorphlineReducer extends Reducer<Text, Text, Text, Text> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MorphlineReducer.class);
    private static final String SEPERATOR = "\0001";
    private Text value;
    private final Record record = new Record();
    private Command morphline;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        File morphLineFile = new File(context.getConfiguration().get(MorphlineMRDriver.MORPHLINE_FILE));
        String morphLineId = context.getConfiguration().get(MorphlineMRDriver.MORPHLINE_ID);
        ReducerRecordEmitter recordEmitter = new ReducerRecordEmitter(context);
        MorphlineContext morphlineContext = new MorphlineContext.Builder().build();
        morphline = new org.kitesdk.morphline.base.Compiler()
                .compile(morphLineFile, morphLineId, morphlineContext, recordEmitter);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        record.put("key", new ByteArrayInputStream(key.toString().getBytes()));
        String strValues = "";
        Iterator<Text> iter = values.iterator();
        while(iter.hasNext()) {
            value = iter.next();
            strValues += value.toString() + SEPERATOR;
        }
        record.put("values", new ByteArrayInputStream(strValues.getBytes()));
        if (!morphline.process(record)) {
            LOGGER.info("Morphline failed to process record: {}", record);
        }
        record.removeAll(Fields.ATTACHMENT_BODY);
    }
}
