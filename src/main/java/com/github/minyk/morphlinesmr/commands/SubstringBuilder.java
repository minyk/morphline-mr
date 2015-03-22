package com.github.minyk.morphlinesmr.commands;

import com.typesafe.config.Config;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import java.util.Collection;
import java.util.Collections;

public final class SubstringBuilder implements CommandBuilder {
    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("subString");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new Substring(this, config, parent, child, context );
    }

    private static final class Substring extends AbstractCommand {

        private final String field;
        private final int start;
        private final int end;
        private final boolean fill;
        private final String fillChar;
        private String value = "";
        private int len;

        protected Substring(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
            super(builder, config, parent, child, context);
            field = getConfigs().getString(config, "field");
            start = getConfigs().getInt(config, "startAt",0);
            end = getConfigs().getInt(config, "endBefore", 1);
            fill = getConfigs().getBoolean(config, "fill", false);
            fillChar = getConfigs().getString(config, "fillChar", " ");
        }

        @Override
        protected boolean doProcess(Record record) {
            value = (String)record.getFirstValue(field);
            len = value.length();
            if(len > end) {
                record.replaceValues(field, value.substring(start, end));
            } else if (fill){
                for(int i=0; i<end-len; i++) {
                    value += fillChar;
                }
                record.replaceValues(field, value.substring(start, end));
            } else {
                return false;
            }
            return super.doProcess(record);
        }

    }
}