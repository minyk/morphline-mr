package com.github.minyk.morphlinesmr.counter;

/**
 * Created by minyk on 11/2/14.
 */
public class MorphlinesMRCounters {

    public static final String COUNTERGROUP = "morphlinesmr";

    public static class Mapper {
        private static final String MAPPER = "mapper-";
        public static final String COUNTER_INPUTTOTAL = MAPPER + "total";
        public static final String COUNTER_EXCEPTIOIN = MAPPER + "exception";
        public static final String COUNTER_PROCESSED = MAPPER + "processed";
    }

    public static class Reducer {
        private static final String REDUCER = "reducer-";
        public static final String COUNTER_INPUTTOTAL = REDUCER + "total";
    }
}
