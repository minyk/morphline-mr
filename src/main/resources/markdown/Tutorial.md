## Running with local mode

## Running in the cluster 

* Should be supply morphline configuration file with --files option.
```
# yarn jar morphline-mr-0.1-jar-with-dependencies.jar --files conf/morphline_with_drop.conf  -f morphline_with_drop.conf -m morphline1 -i /user/root/syslog/messages -o /user/root/syslog/output
```

* Use ExceptionPartitioner,
```
# yarn jar morphline-mr-0.1-jar-with-dependencies.jar --files conf/morphline_with_exception.conf  -f morphline_with_exception.conf -m morphline1 -i /user/root/syslog/messages -o /user/root/syslog/output2 -r
```

then

```
14/10/11 19:10:23 INFO minyk.MorphlineMRDriver: Use reducers: true
14/10/11 19:10:23 INFO minyk.MorphlineMRDriver: Total number of reducers: 10
14/10/11 19:10:23 INFO minyk.MorphlineMRDriver: Total number of exception reducers: 2
14/10/11 19:10:24 INFO client.RMProxy: Connecting to ResourceManager at sandbox.example.com/10.10.10.11:8050
14/10/11 19:10:26 INFO input.FileInputFormat: Total input paths to process : 1
14/10/11 19:10:26 INFO mapreduce.JobSubmitter: number of splits:1
14/10/11 19:10:27 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1413003286416_0009
14/10/11 19:10:27 INFO impl.YarnClientImpl: Submitted application application_1413003286416_0009
14/10/11 19:10:27 INFO mapreduce.Job: The url to track the job: http://sandbox.example.com:8088/proxy/application_1413003286416_0009/
14/10/11 19:10:27 INFO mapreduce.Job: Running job: job_1413003286416_0009
14/10/11 19:10:37 INFO mapreduce.Job: Job job_1413003286416_0009 running in uber mode : false
14/10/11 19:10:37 INFO mapreduce.Job:  map 0% reduce 0%
14/10/11 19:10:46 INFO mapreduce.Job:  map 100% reduce 0%
14/10/11 19:10:59 INFO mapreduce.Job:  map 100% reduce 10%
14/10/11 19:11:00 INFO mapreduce.Job:  map 100% reduce 20%
14/10/11 19:11:09 INFO mapreduce.Job:  map 100% reduce 30%
14/10/11 19:11:11 INFO mapreduce.Job:  map 100% reduce 40%
14/10/11 19:11:20 INFO mapreduce.Job:  map 100% reduce 50%
14/10/11 19:11:21 INFO mapreduce.Job:  map 100% reduce 60%
14/10/11 19:11:31 INFO mapreduce.Job:  map 100% reduce 70%
14/10/11 19:11:33 INFO mapreduce.Job:  map 100% reduce 80%
14/10/11 19:11:42 INFO mapreduce.Job:  map 100% reduce 90%
14/10/11 19:11:43 INFO mapreduce.Job:  map 100% reduce 100%
14/10/11 19:11:43 INFO mapreduce.Job: Job job_1413003286416_0009 completed successfully
14/10/11 19:11:43 INFO mapreduce.Job: Counters: 49
	File System Counters
		FILE: Number of bytes read=152484
		FILE: Number of bytes written=1394624
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=136247
		HDFS: Number of bytes written=127436
		HDFS: Number of read operations=33
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=20
	Job Counters 
		Launched map tasks=1
		Launched reduce tasks=10
		Data-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=6120
		Total time spent by all reduces in occupied slots (ms)=95683
		Total time spent by all map tasks (ms)=6120
		Total time spent by all reduce tasks (ms)=95683
		Total vcore-seconds taken by all map tasks=6120
		Total vcore-seconds taken by all reduce tasks=95683
		Total megabyte-seconds taken by all map tasks=1566720
		Total megabyte-seconds taken by all reduce tasks=24494848
	Map-Reduce Framework
		Map input records=1293
		Map output records=1293
		Map output bytes=149647
		Map output materialized bytes=152484
		Input split bytes=122
		Combine input records=0
		Combine output records=0
		Reduce input groups=26
		Reduce shuffle bytes=152484
		Reduce input records=1293
		Reduce output records=1293
		Spilled Records=2586
		Shuffled Maps =10
		Failed Shuffles=0
		Merged Map outputs=10
		GC time elapsed (ms)=1367
		CPU time spent (ms)=9540
		Physical memory (bytes) snapshot=1638051840
		Virtual memory (bytes) snapshot=7681064960
		Total committed heap usage (bytes)=802492416
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=136125
	File Output Format Counters 
		Bytes Written=127436
```

Confirm output data
```
[root@sandbox morphline-mr-0.1-bin]# hdfs dfs -ls /user/root/syslog/output2
Found 11 items
-rw-r--r--   1 root hdfs          0 2014-10-11 19:11 /user/root/syslog/output2/_SUCCESS
-rw-r--r--   1 root hdfs        439 2014-10-11 19:10 /user/root/syslog/output2/part-r-00000
-rw-r--r--   1 root hdfs      13702 2014-10-11 19:10 /user/root/syslog/output2/part-r-00001
-rw-r--r--   1 root hdfs       7342 2014-10-11 19:11 /user/root/syslog/output2/part-r-00002
-rw-r--r--   1 root hdfs       2925 2014-10-11 19:11 /user/root/syslog/output2/part-r-00003
-rw-r--r--   1 root hdfs      13662 2014-10-11 19:11 /user/root/syslog/output2/part-r-00004
-rw-r--r--   1 root hdfs       1059 2014-10-11 19:11 /user/root/syslog/output2/part-r-00005
-rw-r--r--   1 root hdfs       2427 2014-10-11 19:11 /user/root/syslog/output2/part-r-00006
-rw-r--r--   1 root hdfs      85845 2014-10-11 19:11 /user/root/syslog/output2/part-r-00007
-rw-r--r--   1 root hdfs         35 2014-10-11 19:11 /user/root/syslog/output2/part-r-00008
-rw-r--r--   1 root hdfs          0 2014-10-11 19:11 /user/root/syslog/output2/part-r-00009
[root@sandbox morphline-mr-0.1-bin]# hdfs dfs -cat /user/root/syslog/output2/part-r-00008
Oct 11 17:00:17 sandbox -- MARK --
```