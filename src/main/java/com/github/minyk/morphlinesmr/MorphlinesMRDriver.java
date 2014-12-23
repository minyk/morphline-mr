package com.github.minyk.morphlinesmr;

import com.github.minyk.morphlinesmr.counter.MorphlinesMRCounters;
import com.github.minyk.morphlinesmr.mapper.IgnoreKeyOutputFormat;
import com.github.minyk.morphlinesmr.mapper.MorphlinesMapper;
import com.github.minyk.morphlinesmr.partitioner.ExceptionPartitioner;
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MorphlinesMRDriver extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(MorphlinesMRDriver.class);

    private Options buildOption() {
        Options opts = new Options();
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
        opts.addOption(output);

        Option reduce = new Option("r", "use-reducer", false, "Use map-reduce process.");
        reduce.setRequired(false);
        opts.addOption(reduce);

        Option reducers = new Option("n", "total-reducers",true, "Total number of reducers. Default is 10 reducers");
        reducers.setRequired(false);
        opts.addOption(reducers);

        Option exceptions = new Option("e", "exception-reducers", true, "Total number of reducers for exception cases. Default is 2 reducers");
        exceptions.setRequired(false);
        opts.addOption(exceptions);

        Option jobname = new Option("j", "job-name", true, "Name of the job.");
        jobname.setRequired(false);
        opts.addOption(jobname);

        Option local = new Option("l", "local-mode", false, "Use local mode.");
        local.setRequired(false);
        opts.addOption(local);

        Option dictionaries = new Option("d", "grok-dictionaries", true, "grok dictionaries.");
        dictionaries.setRequired(false);
        opts.addOption(dictionaries);

        Option ganglia = new Option("g", "ganglia", true, "ganglia gmeta server.");
        dictionaries.setRequired(false);
        opts.addOption(ganglia);

        return opts;
    }

    @Override
    public int run(String[] args) throws Exception {

        if(args.length < 1) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("yarn jar morphlines-mr.jar",buildOption());
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(1);
        }

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(buildOption(), args, true);

        Configuration config = this.getConf();

        // Load conf from command line.
        if(cmd.hasOption('i')) {
            config.set(MorphlinesMRConfig.INPUT_PATH, cmd.getOptionValue('i'));
        }

        if(cmd.hasOption('o')) {
            config.set(MorphlinesMRConfig.OUTPUT_PATH, cmd.getOptionValue('o'));
        }

        if(cmd.hasOption('l')) {
            config.set(MorphlinesMRConfig.MORPHLINESMR_MODE, MorphlinesMRConfig.DEFAULT_MORPHLIESMR_MODE);
        }

        if(cmd.hasOption('j')) {
            config.set(MorphlinesMRConfig.JOB_NAME, cmd.getOptionValue('j'));
        }

        if(cmd.hasOption('f')) {
            config.set(MorphlinesMRConfig.MORPHLINE_FILE, cmd.getOptionValue('f'));
        }

        if(cmd.hasOption('m')) {
            config.set(MorphlinesMRConfig.MORPHLINE_ID, cmd.getOptionValue('m'));
        }

        if(cmd.hasOption('n')) {
            config.set(MorphlinesMRConfig.MORPHLINESMR_REDUCERS, cmd.getOptionValue('n', "10"));
        }

        if(cmd.hasOption('e')) {
            config.set(MorphlinesMRConfig.MORPHLINESMR_REDUCERS_EXCEPTION, cmd.getOptionValue('e', "2"));
        }

        if(cmd.hasOption('g')) {
            config.set(MorphlinesMRConfig.METRICS_GANGLAI_SINK, cmd.getOptionValue('g'));
        }

        if(cmd.hasOption('r')) {
            config.set(MorphlinesMRConfig.MORPHLINESMR_REDUCERS, "10");
            config.set(MorphlinesMRConfig.MORPHLINESMR_REDUCERS_EXCEPTION, "2");
        }
        // 1 left

        // Make Job obj.

        Job job = new Job(config);
        job.setJarByClass(MorphlinesMRDriver.class);
        job.setMapperClass(MorphlinesMapper.class);

        Configuration conf = job.getConfiguration();

        if (conf.get(MorphlinesMRConfig.MORPHLINESMR_MODE).equals(MorphlinesMRConfig.DEFAULT_MORPHLIESMR_MODE)) {
            LOGGER.info("Use local mode.");
            conf.set(MRConfig.FRAMEWORK_NAME,MRConfig.LOCAL_FRAMEWORK_NAME);
            conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);

        } else {
            Path morphlinefile = new Path(conf.get(MorphlinesMRConfig.CONF_DIR) + Path.SEPARATOR + conf.get(MorphlinesMRConfig.MORPHLINE_FILE));
            job.addCacheFile(morphlinefile.toUri());

        }

        if(cmd.hasOption('r') || cmd.hasOption('n') || cmd.hasOption('e')) {
            int tr = conf.getInt(MorphlinesMRConfig.MORPHLINESMR_REDUCERS, 10);
            int er = conf.getInt(MorphlinesMRConfig.MORPHLINESMR_REDUCERS_EXCEPTION, 2);

            LOGGER.info("Use reducers: true");
            LOGGER.info("Total number of reducers: " + (tr + er));
            LOGGER.info("Total number of exception reducers: " + er);

            job.setNumReduceTasks((tr+er));
            job.getConfiguration().setInt(ExceptionPartitioner.EXCEPRION_REDUCERS, er);
            job.setPartitionerClass(ExceptionPartitioner.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
        } else {
            LOGGER.info("Use reducers: false");
            job.setNumReduceTasks(0);
        }

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(IgnoreKeyOutputFormat.class);

        String input_path = conf.get(MorphlinesMRConfig.INPUT_PATH);
        String output_path = conf.get(MorphlinesMRConfig.OUTPUT_PATH);

        Path outputPath = new Path(output_path);
        FileSystem fs = FileSystem.get(conf);
        try {
            if(fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            System.exit(1);
        } finally {
            fs.close();
        }

        FileInputFormat.addInputPath(job, new Path(input_path));
        IgnoreKeyOutputFormat.setOutputPath(job, outputPath);

        int result = job.waitForCompletion(true) ? 0 : 1;

        if(result == 0) {
            saveJobLogs(job);
        }

        return result;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new MorphlinesMRDriver(), args );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void saveJobLogs(Job job) throws Exception {

        LOGGER.info("Job Name: " + job.getJobName());

        if(job.isSuccessful()) {
            LOGGER.info("Job Status: Successful.");
            CounterGroup counterGroup = job.getCounters().getGroup(MorphlinesMRCounters.COUNTERGROUP);
            String groupname = counterGroup.getDisplayName();
            for(Counter c : counterGroup) {
                LOGGER.info(groupname + "." +c.getDisplayName() + "=" + c.getValue());
            }
        } else {
            LOGGER.info("Job Status: " + job.getStatus().toString());
        }

    }

}
