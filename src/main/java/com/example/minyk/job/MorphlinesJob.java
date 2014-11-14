package com.example.minyk.job;

import com.example.minyk.counter.MorphlinesMRCounters;
import com.example.minyk.metrics.MorphlinesMRMetrics;
import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetricSlope;
import info.ganglia.gmetric4j.gmetric.GMetricType;
import info.ganglia.gmetric4j.gmetric.GangliaException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class MorphlinesJob extends Job {
    private static final Log LOG = LogFactory.getLog(MorphlinesJob.class);

    private GMetric ganglia;
    private boolean reportGanglia = false;

    private MorphlinesJob() throws IOException {
        super();
    }

    private MorphlinesJob(JobConf jobConf) throws IOException {
        super(jobConf);
    }

    private MorphlinesJob(JobConf jobConf, String server, int port, GMetric.UDPAddressingMode mode ) throws IOException {
        super(jobConf);
        ganglia = new GMetric(server, port, mode, 1);
        reportGanglia = true;
    }

    @Override
    public boolean monitorAndPrintJob() throws IOException, InterruptedException {
        String lastReport = null;
        Job.TaskStatusFilter filter;
        Configuration clientConf = getConfiguration();
        filter = Job.getTaskOutputFilter(clientConf);
        JobID jobId = getJobID();
        LOG.info("Running job: " + jobId);
        int eventCounter = 0;
        boolean profiling = getProfileEnabled();
        Configuration.IntegerRanges mapRanges = getProfileTaskRange(true);
        Configuration.IntegerRanges reduceRanges = getProfileTaskRange(false);
        int progMonitorPollIntervalMillis =
                Job.getProgressPollInterval(clientConf);
    /* make sure to report full progress after the job is done */
        boolean reportedAfterCompletion = false;
        boolean reportedUberMode = false;
        while (!isComplete() || !reportedAfterCompletion) {
            if (isComplete()) {
                reportedAfterCompletion = true;
            } else {
                Thread.sleep(progMonitorPollIntervalMillis);
            }
            if (getStatus().getState() == JobStatus.State.PREP) {
                continue;
            }
            if (!reportedUberMode) {
                reportedUberMode = true;
                LOG.info("Job " + jobId + " running in uber mode : " + isUber());
            }
            String report =
                    (" map " + StringUtils.formatPercent(mapProgress(), 0)+
                            " reduce " +
                            StringUtils.formatPercent(reduceProgress(), 0));
            if (!report.equals(lastReport)) {
                LOG.info(report);

                // Report to ganglia
                if(reportGanglia) {
                    gangliaMetric();
                }
                lastReport = report;
            }

            TaskCompletionEvent[] events =
                    getTaskCompletionEvents(eventCounter, 10);
            eventCounter += events.length;
            printTaskEvents(events, filter, profiling, mapRanges, reduceRanges);
        }
        boolean success = isSuccessful();
        if (success) {
            LOG.info("Job " + jobId + " completed successfully");
        } else {
            LOG.info("Job " + jobId + " failed with state " + getStatus().getState() +
                    " due to: " + getStatus().getFailureInfo());
        }
        Counters counters = getCounters();
        if (counters != null) {
            LOG.info(counters.toString());
        }
        gangliaMetricClean();
        return success;
    }

    private void printTaskEvents(TaskCompletionEvent[] events,
                                 Job.TaskStatusFilter filter, boolean profiling, Configuration.IntegerRanges mapRanges,
                                 Configuration.IntegerRanges reduceRanges) throws IOException, InterruptedException {
        for (TaskCompletionEvent event : events) {
            switch (filter) {
                case NONE:
                    break;
                case SUCCEEDED:
                    if (event.getStatus() ==
                            TaskCompletionEvent.Status.SUCCEEDED) {
                        LOG.info(event.toString());
                    }
                    break;
                case FAILED:
                    if (event.getStatus() ==
                            TaskCompletionEvent.Status.FAILED) {
                        LOG.info(event.toString());
                        // Displaying the task diagnostic information
                        TaskAttemptID taskId = event.getTaskAttemptId();
                        String[] taskDiagnostics = getTaskDiagnostics(taskId);
                        if (taskDiagnostics != null) {
                            for (String diagnostics : taskDiagnostics) {
                                System.err.println(diagnostics);
                            }
                        }
                    }
                    break;
                case KILLED:
                    if (event.getStatus() == TaskCompletionEvent.Status.KILLED){
                        LOG.info(event.toString());
                    }
                    break;
                case ALL:
                    LOG.info(event.toString());
                    break;
            }
        }
    }

    public static MorphlinesJob getInstance() throws IOException {

        return MorphlinesJob.getInstance(new Configuration());
    }

    public static MorphlinesJob getInstance(Configuration conf) throws IOException {
        JobConf jobConf = new JobConf(conf);
        return new MorphlinesJob(jobConf);
    }

    public static MorphlinesJob getInstance(Configuration conf, String server, int port, GMetric.UDPAddressingMode mode) throws IOException {
        JobConf jobConf = new JobConf(conf);
        return new MorphlinesJob(jobConf, server, port, mode);
    }

    public static MorphlinesJob getInstance(Configuration conf, String jobName) throws IOException {
        MorphlinesJob result = getInstance(conf);
        result.setJobName(jobName);
        return result;
    }

    public static MorphlinesJob getInstance(Configuration conf, String jobName, String server, int port, GMetric.UDPAddressingMode mode) throws IOException {
        MorphlinesJob result = getInstance(conf, server, port, mode);
        result.setJobName(jobName);
        return result;
    }

    private void gangliaMetric() {
        int tmax = 60;
        int dmax = 0;
        String jobName = this.getJobName();
        try {
            CounterGroup counterGroup = this.getCounters().getGroup(MorphlinesMRCounters.COUNTERGROUP);
            ganglia.announce(jobName + ".Map.InputRecords", String.valueOf(this.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue()), GMetricType.INT32, "EA", GMetricSlope.BOTH, tmax, dmax, "morphlines-mr");
            ganglia.announce(jobName + ".Reduce.OutputRecords", String.valueOf(this.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue()), GMetricType.INT32, "EA", GMetricSlope.BOTH, tmax, dmax, "morphlines-mr");
            ganglia.announce(MorphlinesMRMetrics.getExceptionRecordName(jobName), String.valueOf(counterGroup.findCounter(MorphlinesMRCounters.Mapper.COUNTER_EXCEPTIOIN).getValue()), GMetricType.INT32, "EA", GMetricSlope.BOTH, tmax, dmax, "morphlines-mr");
            ganglia.announce(MorphlinesMRMetrics.getProcessedRecordName(jobName), String.valueOf(counterGroup.findCounter(MorphlinesMRCounters.Mapper.COUNTER_PROCESSED).getValue()), GMetricType.INT32, "EA", GMetricSlope.BOTH, tmax, dmax, "morphlines-mr");
        } catch (GangliaException e) {
            LOG.warn("Cannot report metric to ganglia: " + e.getMessage());
        } catch (IOException ioe) {
            LOG.warn("Cannot read Counters: " + ioe.getMessage());
        }
    }

    private void gangliaMetricClean() {
        int tmax = 60;
        int dmax = 0;
        String jobName = this.getJobName();
        try {
            ganglia.announce(jobName + ".Map.InputRecords", String.valueOf(0), GMetricType.INT32, "EA", GMetricSlope.BOTH, tmax, dmax, "morphlines-mr");
            ganglia.announce(jobName + ".Reduce.OutputRecords", String.valueOf(0), GMetricType.INT32, "EA", GMetricSlope.BOTH, tmax, dmax, "morphlines-mr");
            ganglia.announce(MorphlinesMRMetrics.getExceptionRecordName(jobName), String.valueOf(0), GMetricType.INT32, "EA", GMetricSlope.BOTH, tmax, dmax, "morphlines-mr");
            ganglia.announce(MorphlinesMRMetrics.getProcessedRecordName(jobName), String.valueOf(0), GMetricType.INT32, "EA", GMetricSlope.BOTH, tmax, dmax, "morphlines-mr");
        } catch (GangliaException e) {
            LOG.warn("Cannot report metric to ganglia: " + e.getMessage());
        }
    }
}
