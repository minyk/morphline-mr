package com.github.minyk.morphlinesmr.commands;

import com.typesafe.config.Config;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import java.util.Collection;
import java.util.Collections;

public class SimpleArithmeticBuilder implements CommandBuilder {
    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("simpleArithmetic");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new Arithmetic(this, config, parent, child, context);
    }

    private static final class Arithmetic extends AbstractCommand {

        private final int plus;
        private final String field;
        private int value;

        protected Arithmetic(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
            super(builder, config, parent, child, context);
            plus = getConfigs().getInt(config, "plus", 0);
            field = getConfigs().getString(config, "field");
        }

        @Override
        protected boolean doProcess(Record record) {
            value = Integer.parseInt((String)record.getFirstValue(field));
            record.replaceValues(field, String.valueOf( value + plus));
            return super.doProcess(record);
        }
    }
}