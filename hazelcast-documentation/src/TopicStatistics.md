


### Statistics

Topic has two statistic variables that can be queried. These values are incremental and local to the member.

```java
HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
ITopic<Object> myTopic = hazelcastInstance.getTopic( "myTopicName" );

myTopic.getLocalTopicStats().getPublishOperationCount();
myTopic.getLocalTopicStats().getReceiveOperationCount();
```


`getPublishOperationCount` and `getReceiveOperationCount` returns total number of publishes and received messages since the start of this node, respectively. Please note that, these values are not backed up and if the node goes down, they will be lost.

This feature can be disabled with topic configuration. Please see [Topic Configuration](#topic-configuration).

![image](images/NoteSmall.jpg) ***NOTE:*** *These statistics values can be also viewed in Management Center. Please see [Topics](#topics)*.




