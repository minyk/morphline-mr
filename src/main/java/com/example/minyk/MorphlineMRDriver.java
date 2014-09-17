package com.example.minyk;

import com.example.minyk.mapper.IgnoreKeyOutputFormat;
import com.example.minyk.mapper.MorphlineMapper;
import com.typesafe.config.ConfigException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MorphlineMRDriver {
    public static final String MORPHLINE_FILE = "morphlineFile";
    public static final String MORPHLINE_ID = "morphlineId";

    public static void main(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(MORPHLINE_FILE, args[2]);
        conf.set(MORPHLINE_ID, args[3]);
        Job job = Job.getInstance(conf, "data cleaning");
        job.setJarByClass(MorphlineMRDriver.class);
        job.setMapperClass(MorphlineMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(IgnoreKeyOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        IgnoreKeyOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
