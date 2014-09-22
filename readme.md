MapReduce with Morphlines.

Morphlne-MR is a simple hadoop map only job for some ETLs. It reads the input data from the Hdfs and do a morphline processing, then write files to the Hdfs. 

Morphline.conf contains all the magic.
* Read data from _attachment_body field
* Grok the string. At this time, input data is splitted up to fields. 
* Each fields are mutable after grok command.
* At the end of process, set the "value" field. 

RecordEmitter is write the data for you. 

This project inspired by SequenceIQ' Mapreduce sample. Thanks for a great example!
