package com.hazelcast.pubsub.impl;

import com.hazelcast.internal.alto.PartitionActorRef;
import com.hazelcast.pubsub.Subscriber;

public class SubscriberImpl implements Subscriber {
    private final String topic;
    private final PartitionActorRef[] partitionActorRefs;

    public SubscriberImpl(String topic, PartitionActorRef[] partitionActorRefs) {
        this.topic = topic;
        this.partitionActorRefs = partitionActorRefs;
    }
}
