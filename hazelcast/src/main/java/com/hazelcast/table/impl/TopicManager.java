package com.hazelcast.table.impl;

import com.hazelcast.pubsub.impl.TopicLog;

import java.io.File;

public class TopicManager {

    private final int partitionCount;
    private final TopicLog[] partitions;
    private final File rootDir = new File("/home/pveentjer/tmp");

    public TopicManager(int partitionCount) {
        this.partitionCount = partitionCount;
        this.partitions = new TopicLog[partitionCount];
    }

    // todo: topic-id should also be passed.
    public TopicLog getLog(int partitionId) {
        TopicLog topicLog = partitions[partitionId];
        if (topicLog == null) {
            topicLog = new TopicLog(partitionId, rootDir);
            partitions[partitionId] = topicLog;
        }
        return topicLog;
    }
}
