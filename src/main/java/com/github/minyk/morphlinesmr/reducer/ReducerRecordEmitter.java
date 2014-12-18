package com.github.minyk.morphlinesmr.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReducerRecordEmitter implements Command {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReducerRecordEmitter.class);
    private final Text output_key = new Text();
    private final Text output_value = new Text();
    private final Reducer.Context context;

    public ReducerRecordEmitter(Reducer.Context context) {
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
        } catch (Exception e) {
            LOGGER.warn("Cannot write record to context", e);
        }
        return true;
    }
}
