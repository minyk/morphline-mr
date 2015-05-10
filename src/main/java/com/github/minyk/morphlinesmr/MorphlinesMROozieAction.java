package com.github.minyk.morphlinesmr;

import com.github.minyk.morphlinesmr.counter.MorphlinesMRCounters;
import com.github.minyk.morphlinesmr.job.MorphlinesJob;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.joda.time.DateTime;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class MorphlinesMROozieAction {

    private static JobConf conf;

    public void main(String[] args) throws Exception {
        conf = new JobConf(System.getProperty("oozie.action.conf.xml"));
        MorphlinesJob job = new MorphlinesMRDriver().run(conf);

        if(job.isSuccessful()) {
            String countersFile = System.getProperty("oozie.action.output.properties");
            File file = new File(countersFile);

            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("#" + DateTime.now().toString() + "\n");
            CounterGroup counterGroup = job.getCounters().getGroup(MorphlinesMRCounters.COUNTERGROUP);
            String groupname = counterGroup.getDisplayName();
            for(Counter c : counterGroup) {
                bw.write(groupname + "." + c.getDisplayName() + "=" + c.getValue() + "\n");
            }
            bw.close();
        } else {
            throw new RuntimeException("Job is failed. See the log of " + job.getJobID());
        }
    }
}
