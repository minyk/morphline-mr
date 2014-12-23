package com.github.minyk.morphlinesmr.mapper;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import com.github.minyk.morphlinesmr.MRJobConfig;
import com.github.minyk.morphlinesmr.MorphlinesMRConfig;
import com.github.minyk.morphlinesmr.partitioner.ExceptionPartitioner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MorphlinesMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MorphlinesMapper.class);
    public static final String EXCEPTION_KEY_FIELD = "exceptionkey";
    private final Record record = new Record();
    private Command morphline;
    boolean useReducers;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        File morphLineFile = new File(context.getConfiguration().get(MorphlinesMRConfig.MORPHLINE_FILE));
        String morphLineId = context.getConfiguration().get(MorphlinesMRConfig.MORPHLINE_ID);
        MapperRecordEmitter recordEmitter = new MapperRecordEmitter(context);
        MorphlineContext morphlineContext = new MorphlineContext.Builder().build();
        morphline = new org.kitesdk.morphline.base.Compiler()
                .compile(morphLineFile, morphLineId, morphlineContext, recordEmitter);
        if(context.getConfiguration().getInt(MRJobConfig.NUM_REDUCES,0) == 0) {
            useReducers = false;
        } else {
            useReducers = true;
        }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        record.put(Fields.ATTACHMENT_BODY, new ByteArrayInputStream(value.toString().getBytes()));

        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("Value: " + value.toString());
        }
        if(useReducers) {
            record.put(EXCEPTION_KEY_FIELD, ExceptionPartitioner.EXCEPTION_KEY);
        }

        if (!morphline.process(record)) {
            LOGGER.info("Morphline failed to process record: {}", record);
        }

        //record.removeAll(Fields.ATTACHMENT_BODY);
        record.getFields().clear();
    }
}
