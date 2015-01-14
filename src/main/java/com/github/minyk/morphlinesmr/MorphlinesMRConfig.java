package com.github.minyk.morphlinesmr;

/**
 * Created by minyk on 11/26/14.
 */
public class MorphlinesMRConfig {

    public static final String MORPHLINE_FILE = "morphlines-mr.conf.filename";
    public static final String MORPHLINE_ID = "morphlines-mr.conf.morphlineid";

    public static final String CONF_DIR = "morphlines-mr.dirs.confs";
    public static final String INPUT_PATH = "morphlines-mr.dirs.input";
    public static final String OUTPUT_PATH = "morphlines-mr.dirs.output";
    public static final String EXCEPTION_PATH = "morphlines-mr.dirs.exception";
    public static final String COUNTER_PATH = "morphlines-mr.dirs.counters";

    public static final String EXCEPTION_PATH_DEFAULT = "NOT USED";

    // local dir
    public static final String LOG_DIR = "morphlines-mr.metrics.log.dir";

    public static final String JOB_NAME = "morphlines-mr.conf.jobname";
    public static final String JOB_NAME_DEFALUT = "morphlines-mr";

    public static final String METRICS_GANGLAI_SINK = "morphlines-mr.metrcis.ganglia-server";

    public static final String MORPHLINESMR_MODE = "morphlines-mr.conf.mode";
    public static final String MORPHLIESMR_MODE_LOCAL = "local";
    public static final String MORPHLIESMR_MODE_MR = "mapred";

    public static final String MORPHLINESMR_REDUCERS = "morphlines-mr.conf.reducers.normal";
    public static final int DEFAULT_MORPHLINESMR_REDUCERS = 0;

    public static final String MORPHLINESMR_REDUCERS_EXCEPTION = "morphlines-mr.conf.reducers.exception";
    public static final int DEFAULT_MORPHLINESMR_REDUCERS_EXCEPTION = 0;

    public static final String MORPHLINESMR_LOCAL_HOME = "MORPHLINEMR_HOME";
    public static final String MORPHLINESMR_DEFAULT_CONFFILE = "morphlinesmr-default.xml";
    public static final String MORPHLINESMR_SITE_CONFFILE = "morphlinesmr-site.xml";

}
