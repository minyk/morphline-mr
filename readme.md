MapReduce with Morphlines
==========================

## Introduction

Morphlne-MR is a simple hadoop map only job for some ETLs. It reads the input data from the Hdfs and do a morphline processing, then write files to the Hdfs. 

Morphline.conf contains all the magic.
* Read data from _attachment_body field
* Grok the string. At this time, input data is splitted up to fields. 
* Each fields are mutable after grok command.
* At the end of process, set the "value" field. 

RecordEmitter is write the data for you. 

## Options

- "-f" or "--morphline-file" indicates which mophline configuration file is used for.
- "-m" or "--morphlie-id" indicates target morphline id in the configuration file. 
- "-i" or "--input" the input file or directory location.
- "-o" or "--output" the output directory location.
- "-r" or "--use-reducer" if this option is declared, the job uses exception partitioner and reducers. Default is false.
- "-n" or "--total-reducers" describes total number of reducers. Default is 10 reducers.
- "-e" or "--exception-reducers" describes total number of reducers for exception cases. Default is 2.
- "-j" or "--job-name" the name of the job. Default is "Data_Cleaning_Job".
- "-l" or "--local-mode" Use local mode instead of YARN(MRv2) or Classic(MRv1). Use this option for testing purpose.

## Acknowledgement

This project is inspired by SequenceIQ' Mapreduce sample. Thanks for a great example!
