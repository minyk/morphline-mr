package com.github.minyk.morphlinesmr.metrics;

public class MorphlinesMRMetrics {
    public static final String GROUP_NAME = "morphlines-mr";

    private static final String INPUT_RECORD = "Record.Input";
    private static final String EXCEPTION_RECORD = "Record.Exception";
    private static final String PROCESSED_RECORD = "Record.Processed";

    public static String getInputRecordName(String jobName) {
        return jobName + "." + INPUT_RECORD;
    }

    public static String getExceptionRecordName(String jobName) {
        return jobName + "." + EXCEPTION_RECORD;
    }

    public static String getProcessedRecordName(String jobName) {
        return jobName + "." + PROCESSED_RECORD;
    }



}
