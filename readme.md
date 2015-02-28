MapReduce with Morphlines
==========================

## Introduction

Morphlne-MR is a simple hadoop mapreduce job for some ETLs. It reads the input data from the Hdfs and do a morphline processing, then write files to the Hdfs. 

morphline.conf contains all the magic.
* Read data from `_attachment_body` field
* Grok the string. At this time, input data is splitted up to fields. 
* Each fields are mutable after grok command.
* At the end of process, set the `value` field. 

RecordEmitter is write the data for you. 

## Getting Started

```
$ git clone https://github.com/minyk/morphline-mr.git
$ mvn package
$ yarn jar target/morphline-mr-*-jar-with-dependencies.jar -f morphline_with_exception.conf -m morphline1 -i file:///var/log/messages -o file:///root/test/ -l
```
* This command read local syslog file at `/var/log/message`, then output will be located at `/root/test`
 * The output file is full of '1' line.
* `morphline_with_exception.conf` is found in src/main/resource/conf. In later version, bin.tar.gz with all the resources will be provided.

## Options

- "-f" or "--morphline-file" indicates which mophline configuration file is used for. Required.
- "-m" or "--morphlie-id" indicates target morphline id in the configuration file. Required.
- "-i" or "--input" the input file or directory location. Required.
- "-o" or "--output" the output directory location. Required.
- "-e" or "--exception" the exceptiondirectory location. 
- "-r" or "--use-reducer" if this option is declared, the job uses exception partitioner and reducers. Default is false.
- "-n" or "--total-reducers" describes total number of reducers. Default is 10 reducers.
- "-x" or "--exception-reducers" describes total number of reducers for exception cases. Default is 2.
- "-j" or "--job-name" the name of the job. Default is "Data\_Cleaning\_Job".
- "-l" or "--local-mode" Use local mode instead of YARN(MRv2) or Classic(MRv1). Use this option for testing purpose.
- "-g" or "--ganglia" for the ganglia server to report metrics. It can be mutlicast ip.

### Import options from hadoop common option

See wiki: Import options from hadoop common option page.

## Map only job

If one of 'r' or 'n' or 'e' options are not provided, the driver run map-only job. In this case, unparsable records should be dropped with dropRecord command.

## How to use Exception Partitioner

If number of reducers is not 0, the driver sets ExceptionPartitioner and IdentityReducer. In this case, morphline records have `exceptionkey` field for value of exception key.

In the morphline config files, use this value to key when exception is thrown. For normal records, proper value should be set in key field for partitioning.

See https://github.com/minyk/morphline-mr/wiki/Grok-with-tryRules

## Grok dictionary references

```
mvn clean process-sources
```

The html pages are in `target/html/`.

## Acknowledgements

This project is inspired by SequenceIQ' Mapreduce sample. Thanks for a great example!
