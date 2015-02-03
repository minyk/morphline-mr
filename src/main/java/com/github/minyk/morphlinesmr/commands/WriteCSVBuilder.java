package com.github.minyk.morphlinesmr.commands;


import com.github.minyk.morphlinesmr.shaded.com.googlecode.jcsv.CSVStrategy;
import com.github.minyk.morphlinesmr.shaded.com.googlecode.jcsv.writer.CSVWriter;
import com.github.minyk.morphlinesmr.shaded.com.googlecode.jcsv.writer.internal.CSVWriterBuilder;
import com.github.minyk.morphlinesmr.shaded.com.googlecode.jcsv.writer.internal.DefaultCSVEntryConverter;
import com.typesafe.config.Config;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by minyk on 2/3/15.
 */
public final class WriteCSVBuilder implements CommandBuilder {
    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("writeCSV");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new WriteCSV(this, config, parent, child, context);
    }

    private static final class WriteCSV extends AbstractCommand {

        private final CSVStrategy csvStrategy;
        private final CSVWriter csvWriter;
        private final StringWriter sw;
        private final String output_field;
        private final List<String> input_fields;
        private final int input_size;
        private final String[] results;

        protected WriteCSV(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
            super(builder, config, parent, child, context);
            char delimiter = getConfigs().getString(config, "delimiter", ",").charAt(0);
            input_fields = getConfigs().getStringList(config,"inputs");
            input_size = input_fields.size();
            results = new String[input_size];

            output_field = getConfigs().getString(config,"output", "output");
            csvStrategy = new CSVStrategy(delimiter, '"', '#', false, false);

            sw = new StringWriter();
            CSVWriterBuilder<String[]> csvWriterBuilder = new CSVWriterBuilder<String[]>(sw);
            csvWriterBuilder.entryConverter(new DefaultCSVEntryConverter());
            csvWriterBuilder.strategy(csvStrategy);
            csvWriter = csvWriterBuilder.build();
        }

        @Override
        protected boolean doProcess(Record record) {
            for (int i=0; i < input_size; i++) {
                results[i] = (String) record.getFirstValue(input_fields.get(i));
            }
            record.getFields().put(output_field, csvWriter.writeString(results));
            return super.doProcess(record);
        }
    }
}
