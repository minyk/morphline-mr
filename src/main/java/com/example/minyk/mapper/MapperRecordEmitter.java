package com.example.minyk.mapper;

import com.example.minyk.counter.MorphlinesMRCounters;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperRecordEmitter implements Command {
    private static final Logger LOGGER = LoggerFactory.getLogger(MapperRecordEmitter.class);
    private final Text output_key = new Text();
    private final Text output_value = new Text();
    private final Mapper.Context context;

    public MapperRecordEmitter(Mapper.Context context) {
        this.context = context;
    }

    @Override
    public void notify(Record notification) {
    }

    @Override
    public Command getParent() {
        return null;
    }

    @Override
    public boolean process(Record record) {
        output_key.set(record.get("key").get(0).toString());
        output_value.set(record.get("value").get(0).toString());
        try {
            context.write(output_key, output_value);
            if(output_key.toString().equals(MorphlineMapper.EXCEPTION_KEY_FIELD)) {
                context.getCounter(MorphlinesMRCounters.COUNTERGROUP, MorphlinesMRCounters.Mapper.COUNTER_EXCEPTIOIN).increment(1L);
            } else {
                context.getCounter(MorphlinesMRCounters.COUNTERGROUP, MorphlinesMRCounters.Mapper.COUNTER_PROCESSED).increment(1L);
            }
        } catch (Exception e) {
            LOGGER.warn("Cannot write record to context", e);
        }
        return true;
    }
}
