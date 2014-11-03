package com.example.minyk.job;

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetricSlope;
import info.ganglia.gmetric4j.gmetric.GMetricType;
import info.ganglia.gmetric4j.gmetric.GangliaException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by drake on 11/3/14.
 */
public class MorphlinesJob extends Job {
    private static final Log LOG = LogFactory.getLog(MorphlinesJob.class);

    private String metric_sink;
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
            try {
                ganglia.announce("MapProgress", String.valueOf(this.mapProgress()), GMetricType.FLOAT, "%", GMetricSlope.POSITIVE, 100, 100, "morphlines-mr");
            } catch (GangliaException e) {
                LOG.warn("Cannot report metric to ganglia: " + e.getMessage());
            }
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

    public void setMetricSink(String url) throws  IOException {
        this.metric_sink = new String(url);
        ganglia = new GMetric(url, 8649, GMetric.UDPAddressingMode.UNICAST, 1);
    }
}
