package com.hazelcast.partition;

import com.hazelcast.core.MigrationListener;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.EventListener;

/**
 * PartitionEventListener is a base interface for partition-related event listeners
 *
 * @param <T> A partition-related event class
 * @see MigrationListener
 * @see PartitionLostListener
 */
@PrivateApi
public interface PartitionEventListener<T extends PartitionEvent>
        extends EventListener {

    void onEvent(T event);

}
