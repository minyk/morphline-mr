package com.example.minyk;

import com.example.minyk.mapper.IgnoreKeyOutputFormat;
import com.example.minyk.mapper.MorphlineMapper;
import com.typesafe.config.ConfigException;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MorphlineMRDriver extends Configured implements Tool {
    public static final String MORPHLINE_FILE = "morphlineFile";
    public static final String MORPHLINE_ID = "morphlineId";

    private Options opts;

    private Options buildOption() {
        opts = new Options();
        Option mfile = new Option("f", "morphline-file", true, "target morphline file.");
        mfile.setRequired(true);
        opts.addOption(mfile);

        Option mid = new Option("m", "morphlie-id", true, "target morphline id in the file");
        mid.setRequired(true);
        opts.addOption(mid);

        Option input = new Option("i", "input", true, "input location");
        input.setRequired(true);
        opts.addOption(input);

        Option output = new Option("o", "output", true, "output location");
        input.setRequired(true);
        opts.addOption(input);

        return opts;
    }

    @Override
    public int run(String[] args) throws Exception {
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(buildOption(), args, true);

        String filename = cmd.getOptionValue('f');
        String id = cmd.getOptionValue('m');
        String input_path = cmd.getOptionValue('i');
        String output_path = cmd.getOptionValue('o');

        Configuration conf = this.getConf();
        conf.set(MORPHLINE_FILE, filename);
        conf.set(MORPHLINE_ID, id);
        Job job = Job.getInstance(conf, "data cleaning");
        job.setJarByClass(MorphlineMRDriver.class);
        job.setMapperClass(MorphlineMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(IgnoreKeyOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input_path));
        IgnoreKeyOutputFormat.setOutputPath(job, new Path(output_path));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
