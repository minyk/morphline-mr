package com.example.minyk;

import com.example.minyk.counter.MorphlinesMRCounters;
import com.example.minyk.mapper.IgnoreKeyOutputFormat;
import com.example.minyk.mapper.MorphlineMapper;
import com.example.minyk.partitioner.ExceptionPartitioner;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MorphlineMRDriver extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(MorphlineMRDriver.class);

    public static final String MORPHLINE_FILE = "morphlineFile";
    public static final String MORPHLINE_ID = "morphlineId";

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

        return opts;
    }

    @Override
    public int run(String[] args) throws Exception {

        if(args.length < 1) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("hadoop jar Morphline-mr.jar",buildOption());
            System.exit(1);
        }

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(buildOption(), args, true);

        String filename = cmd.getOptionValue('f');
        String id = cmd.getOptionValue('m');
        String input_path = cmd.getOptionValue('i');
        String output_path = cmd.getOptionValue('o');

        String job_name = "";
        if(cmd.hasOption('j')) {
            job_name = cmd.getOptionValue('j');
        } else {
            job_name = "Data_Cleaning_Job";
        }

        Job job = Job.getInstance(this.getConf(), job_name);
        job.setJarByClass(MorphlineMRDriver.class);
        job.setMapperClass(MorphlineMapper.class);

        Configuration conf = job.getConfiguration();
        conf.set(MORPHLINE_ID, id);

        if (cmd.hasOption('l')) {
            LOGGER.info("Use local mode.");
            conf.set(MRConfig.FRAMEWORK_NAME,MRConfig.LOCAL_FRAMEWORK_NAME);
            conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);
            conf.set(MORPHLINE_FILE, filename);
        } else {

            // Add morphline file to distributed cache
//            LocalFileSystem fs = FileSystem.getLocal(conf);
            Path morphlinefile = new Path(filename);
            job.addCacheFile(morphlinefile.toUri());
            conf.set(MORPHLINE_FILE, morphlinefile.getName());
//        if(cmd.hasOption('d')) {
//            // Add grok dictionaries to distributed cache
//            Path grokdic = new Path(cmd.getOptionValue('d'));
//            if (lfs.isDirectory(grokdic)) {
//                for(RemoteIterator<LocatedFileStatus> iter = lfs.listFiles(grokdic,false); iter.hasNext(); ) {
//                    LocatedFileStatus fileStatus = iter.next();
//                    job.addFileToClassPath(fileStatus.getPath());
//                }
//            } else {
//                job.addCacheFile(grokdic.toUri());
//            }
//        }
        }


        if(cmd.hasOption('r') || cmd.hasOption('n') || cmd.hasOption('e')) {
            int tr = Integer.parseInt(cmd.getOptionValue('n', "10"));
            int er = Integer.parseInt(cmd.getOptionValue('e', "2"));
            if(er >= tr) {
                LOGGER.error("Total number of reducers should be larger than the number of exception reducers. " + tr + "is smaller than " + er + ".");
                System.exit(1);
            }
            LOGGER.info("Use reducers: true");
            LOGGER.info("Total number of reducers: " + tr);
            LOGGER.info("Total number of exception reducers: " + er);
            job.setNumReduceTasks(tr);
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

        Path outputPath = new Path(output_path);
        FileSystem fs = FileSystem.get(job.getConfiguration());
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
