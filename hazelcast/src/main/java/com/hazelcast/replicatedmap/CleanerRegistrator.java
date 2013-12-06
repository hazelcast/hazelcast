package com.hazelcast.replicatedmap;

import com.hazelcast.replicatedmap.record.AbstractReplicatedRecordStorage;

import java.util.concurrent.ScheduledFuture;

public interface CleanerRegistrator {

    <V> ScheduledFuture<V> registerCleaner(AbstractReplicatedRecordStorage replicatedRecordStorage);

}
