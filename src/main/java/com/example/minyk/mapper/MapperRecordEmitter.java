package com.example.minyk.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperRecordEmitter implements Command {
    private static final Logger LOGGER = LoggerFactory.getLogger(MapperRecordEmitter.class);
    private final Text line = new Text();
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
        line.set(record.get("_attachment_body").get(0).toString());
        try {
            context.write(line, null);
        } catch (Exception e) {
            LOGGER.warn("Cannot write record to context", e);
        }
        return true;
    }
}
