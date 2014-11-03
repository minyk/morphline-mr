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
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by drake on 11/3/14.
 */
public class MorphlinesJob extends Job {
    private static final Log LOG = LogFactory.getLog(MorphlinesJob.class);

    private GMetric ganglia;

    private MorphlinesJob() throws IOException {
        super();
    }

    private MorphlinesJob(JobConf jobConf) throws IOException {
        super(jobConf);
    }

    @Override
    public boolean monitorAndPrintJob() throws IOException, InterruptedException {
        if(ganglia != null) {
            gangliaMetric();
        }
        return super.monitorAndPrintJob();
    }

    public static MorphlinesJob getInstance() throws IOException {

        return MorphlinesJob.getInstance(new Configuration());
    }

    public static MorphlinesJob getInstance(Configuration conf) throws IOException {
        JobConf jobConf = new JobConf(conf);
        return new MorphlinesJob(jobConf);
    }

    public static MorphlinesJob getInstance(Configuration conf, String jobName) throws IOException {
        MorphlinesJob result = getInstance(conf);
        result.setJobName(jobName);
        return result;
    }

    public void setMetricSink(String url, int port, GMetric.UDPAddressingMode mode) throws  IOException {
        ganglia = new GMetric(url, port, mode, 1);
    }

    public void gangliaMetric() {
        int tmax = Job.getProgressPollInterval(this.conf);
        String jobName = this.getJobName();
        try {
            CounterGroup counterGroup = this.getCounters().getGroup(MorphlinesMRCounters.COUNTERGROUP);
            ganglia.announce(jobName + ".Map.Progress", String.valueOf(this.mapProgress()), GMetricType.FLOAT, "%", GMetricSlope.BOTH, tmax, tmax, "morphlines-mr");
            ganglia.announce(jobName + ".Reduce.Progress", String.valueOf(this.reduceProgress()), GMetricType.FLOAT, "%", GMetricSlope.BOTH, tmax, tmax, "morphlines-mr");
            ganglia.announce(MorphlinesMRMetrics.getInputRecordName(jobName), String.valueOf(counterGroup.findCounter(MorphlinesMRCounters.Mapper.COUNTER_INPUTTOTAL).getValue()), GMetricType.INT32, "EA", GMetricSlope.BOTH, tmax, tmax, "morphlines-mr");
            ganglia.announce(MorphlinesMRMetrics.getExceptionRecordName(jobName), String.valueOf(counterGroup.findCounter(MorphlinesMRCounters.Mapper.COUNTER_EXCEPTIOIN).getValue()), GMetricType.INT32, "EA", GMetricSlope.BOTH, tmax, tmax, "morphlines-mr");
            ganglia.announce(MorphlinesMRMetrics.getProcessedRecordName(jobName), String.valueOf(counterGroup.findCounter(MorphlinesMRCounters.Mapper.COUNTER_PROCESSED).getValue()), GMetricType.INT32, "EA", GMetricSlope.BOTH, tmax, tmax, "morphlines-mr");
        } catch (GangliaException e) {
            LOG.warn("Cannot report metric to ganglia: " + e.getMessage());
        } catch (IOException ioe) {
            LOG.warn("Cannot read Counters: " + ioe.getMessage());
        }
    }
}
