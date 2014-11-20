## Use Exception Partitioner

If number of reducers is not 0, the driver sets ExceptionPartitioner and IdentityReducer. In this case, morphline records have `exceptionkey` field for the value of exception key.

In the morphline config files, use this value to key when exception is thrown. For normal records, proper value should be set in `key` field for partitioning.

See https://github.com/minyk/morphline-mr/wiki/Grok-with-tryRules

To provide number of any reducer, use '-r' for default setting(10 total reducers, 2 exception reducers) or '-n' for specifying total number of reducer and default 2 exception reducers or '-e' for describing number of exception reducers and default 10 total reducers. See below:

```
-r 
-n 20
-e 4
-n 20 -e 5
```

## Pick a reducer in exception case

By default, the ExceptionPartitioner uses current time as a partition key. But if needed, set `key` field in morphline with 'exceptionkey' + some string and the partitioner uses the rest part of `key` field as partition key.