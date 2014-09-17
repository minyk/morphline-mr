package com.example.minyk.mapper;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import com.example.minyk.MorphlineMRDriver;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MorphlineMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Mapper.class);
    private final Record record = new Record();
    private Command morphline;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        File morphLineFile = new File(context.getConfiguration().get(MorphlineMRDriver.MORPHLINE_FILE));
        String morphLineId = context.getConfiguration().get(MorphlineMRDriver.MORPHLINE_ID);
        MapperRecordEmitter recordEmitter = new MapperRecordEmitter(context);
        MorphlineContext morphlineContext = new MorphlineContext.Builder().build();
        morphline = new org.kitesdk.morphline.base.Compiler()
                .compile(morphLineFile, morphLineId, morphlineContext, recordEmitter);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        record.put(Fields.ATTACHMENT_BODY, new ByteArrayInputStream(value.toString().getBytes()));
        if (!morphline.process(record)) {
            LOGGER.info("Morphline failed to process record: {}", record);
        }
        record.removeAll(Fields.ATTACHMENT_BODY);
    }

}
