## LearningFlink
learn and do some research on flink

## submit the word count example
[Flink getting started](https://ci.apache.org/projects/flink/flink-docs-release-1.2/quickstart/setup_quickstart.html)

1.build the project

```
mvn clean package -DskipTests
```

2.start flink and run example
```shell
brew install bash # install bash4
./bin/start-local.sh
./bin/flink run --class "com.learning.flink.examples.SimpleWordCount" learning-flink-1.0-SNAPSHOT.jar --port 9000
```

the output is
```
Submitting job with JobID: 3d2c22c0ac59c8c795a8e5b5ba6d7d4b. Waiting for job completion.
Connected to JobManager at Actor[akka.tcp://flink@localhost:6123/user/jobmanager#1312774243]
05/09/2017 09:52:26	Job execution switched to status RUNNING.
05/09/2017 09:52:26	Source: Socket Stream -> Flat Map -> Map(1/1) switched to SCHEDULED 
05/09/2017 09:52:26	Source: Socket Stream -> Flat Map -> Map(1/1) switched to DEPLOYING 
05/09/2017 09:52:26	TriggerWindow(SlidingProcessingTimeWindows(5000, 1000), ReducingStateDescriptor{serializer=com.learning.flink.examples.SimpleWordCount$$anon$2$$anon$1@f5c27d73, reduceFunction=org.apache.flink.streaming.api.functions.aggregation.SumAggregator@35b74c5c}, ProcessingTimeTrigger(), WindowedStream.reduce(WindowedStream.java:276)) -> Sink: Unnamed(1/1) switched to SCHEDULED 
05/09/2017 09:52:26	TriggerWindow(SlidingProcessingTimeWindows(5000, 1000), ReducingStateDescriptor{serializer=com.learning.flink.examples.SimpleWordCount$$anon$2$$anon$1@f5c27d73, reduceFunction=org.apache.flink.streaming.api.functions.aggregation.SumAggregator@35b74c5c}, ProcessingTimeTrigger(), WindowedStream.reduce(WindowedStream.java:276)) -> Sink: Unnamed(1/1) switched to DEPLOYING 
05/09/2017 09:52:27	TriggerWindow(SlidingProcessingTimeWindows(5000, 1000), ReducingStateDescriptor{serializer=com.learning.flink.examples.SimpleWordCount$$anon$2$$anon$1@f5c27d73, reduceFunction=org.apache.flink.streaming.api.functions.aggregation.SumAggregator@35b74c5c}, ProcessingTimeTrigger(), WindowedStream.reduce(WindowedStream.java:276)) -> Sink: Unnamed(1/1) switched to RUNNING 
05/09/2017 09:52:27	Source: Socket Stream -> Flat Map -> Map(1/1) switched to RUNNING 
```

```
tail log/flink-lgrcyanny-jobmanager-*.local.out 
```

we can see the output

```
WordWithCount(now,1)
WordWithCount(run,1)
WordWithCount(flink,1)
WordWithCount(run,1)
WordWithCount(testing,1)
WordWithCount(now,1)
WordWithCount(testing,1)
WordWithCount(now,1)
WordWithCount(run,1)
WordWithCount(flink,1)
```