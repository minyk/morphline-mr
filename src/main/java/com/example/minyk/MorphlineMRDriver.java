package com.example.minyk;

import com.example.minyk.counter.MorphlinesMRCounters;
import com.example.minyk.job.MorphlinesJob;
import com.example.minyk.mapper.IgnoreKeyOutputFormat;
import com.example.minyk.mapper.MorphlineMapper;
import com.example.minyk.partitioner.ExceptionPartitioner;
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

import java.net.URL;

public class MorphlineMRDriver extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(MorphlineMRDriver.class);

    private Options buildOption() {
        Options opts = new Options();
        Option mfile = new Option("f", "morphline-file", true, "target morphline file.");
        mfile.setRequired(false);
        opts.addOption(mfile);

        Option mid = new Option("m", "morphlie-id", true, "target morphline id in the file");
        mid.setRequired(false);
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
            formatter.printHelp("hadoop jar Morphline-mr.jar",buildOption());
            System.exit(1);
        }

        if(!System.getenv().containsKey(MorphlineMRConfig.MORPHLINESMR_LOCAL_HOME)) {
            LOGGER.error("Please set " + MorphlineMRConfig.MORPHLINESMR_LOCAL_HOME + " variable.");
            System.exit(1);
        }

        String MMR_HOME = System.getenv(MorphlineMRConfig.MORPHLINESMR_LOCAL_HOME);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(buildOption(), args, true);

        Configuration config = this.getConf();
        // Load conf from default.xml
        URL file;
        file = MorphlineMRDriver.class.getClassLoader().getResource(MorphlineMRConfig.MORPHLINESMR_DEFAULT_CONFFILE);
        config.addResource(file);

        // Load conf from site.xml
        config.addResource(MMR_HOME + Path.SEPARATOR + "conf" + Path.SEPARATOR + MorphlineMRConfig.MORPHLINESMR_SITE_CONFFILE);

        // Load conf from command line.
        if(cmd.hasOption('i')) {
            config.set(MorphlineMRConfig.INPUT_PATH, cmd.getOptionValue('i'));
        }

        if(cmd.hasOption('o')) {
            config.set(MorphlineMRConfig.OUTPUT_PATH, cmd.getOptionValue('o'));
        }

        if(cmd.hasOption('l')) {
            config.set(MorphlineMRConfig.MORPHLINESMR_MODE, MorphlineMRConfig.DEFAULT_MORPHLIESMR_MODE);
        }

        if(cmd.hasOption('j')) {
            config.set(MorphlineMRConfig.JOB_NAME, cmd.getOptionValue('j'));
        }

        if(cmd.hasOption('f')) {
            config.set(MorphlineMRConfig.MORPHLINE_FILE, cmd.getOptionValue('f'));
        }

        if(cmd.hasOption('m')) {
            config.set(MorphlineMRConfig.MORPHLINE_ID, cmd.getOptionValue('m'));
        }

        if(cmd.hasOption('n')) {
            config.set(MorphlineMRConfig.MORPHLINESMR_REDUCERS, cmd.getOptionValue('n', "10"));
        }

        if(cmd.hasOption('e')) {
            config.set(MorphlineMRConfig.MORPHLINESMR_REDUCERS_EXCEPTION, cmd.getOptionValue('e', "2"));
        }

        if(cmd.hasOption('g')) {
            config.set(MorphlineMRConfig.METRICS_GANGLAI_SINK, cmd.getOptionValue('g'));
        }

        if(cmd.hasOption('r')) {
            config.set(MorphlineMRConfig.MORPHLINESMR_REDUCERS, "10");
            config.set(MorphlineMRConfig.MORPHLINESMR_REDUCERS_EXCEPTION, cmd.getOptionValue('e'));
        }
        // 1 left

        // Make Job obj.
        MorphlinesJob job;

        if(!config.get(MorphlineMRConfig.METRICS_GANGLAI_SINK).isEmpty()) {
            String ganglia_server = config.get(MorphlineMRConfig.METRICS_GANGLAI_SINK);
            LOGGER.info("Use ganglia: " + ganglia_server);
            if(ganglia_server.contains(":")) {
                String[] server = ganglia_server.split("\\:");
                job = MorphlinesJob.getInstance(config, config.get(MorphlineMRConfig.JOB_NAME), server[0], Integer.parseInt(server[1]), GMetric.UDPAddressingMode.getModeForAddress(server[0]) );
            } else {
                job = MorphlinesJob.getInstance(config, config.get(MorphlineMRConfig.JOB_NAME), ganglia_server, 8649, GMetric.UDPAddressingMode.getModeForAddress(ganglia_server));
            }
        } else {
            job = MorphlinesJob.getInstance(this.getConf(), config.get(MorphlineMRConfig.JOB_NAME));
        }

        job.setJarByClass(MorphlineMRDriver.class);
        job.setMapperClass(MorphlineMapper.class);

        Configuration conf = job.getConfiguration();

        if (conf.get(MorphlineMRConfig.MORPHLINESMR_MODE).equals(MorphlineMRConfig.DEFAULT_MORPHLIESMR_MODE)) {
            LOGGER.info("Use local mode.");
            conf.set(MRConfig.FRAMEWORK_NAME,MRConfig.LOCAL_FRAMEWORK_NAME);
            conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);

        } else {
            Path morphlinefile = new Path(conf.get(MorphlineMRConfig.CONF_DIR) + Path.SEPARATOR + conf.get(MorphlineMRConfig.MORPHLINE_FILE));
            job.addCacheFile(morphlinefile.toUri());
        }

        if(cmd.hasOption('r') || cmd.hasOption('n') || cmd.hasOption('e')) {
            int tr = conf.getInt(MorphlineMRConfig.MORPHLINESMR_REDUCERS, 10);
            int er = conf.getInt(MorphlineMRConfig.MORPHLINESMR_REDUCERS_EXCEPTION, 2);

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

        String input_path = conf.get(MorphlineMRConfig.INPUT_PATH);
        String output_path = conf.get(MorphlineMRConfig.OUTPUT_PATH);

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
            ToolRunner.run(new MorphlineMRDriver(), args );
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
