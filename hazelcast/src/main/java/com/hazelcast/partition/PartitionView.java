package com.hazelcast.partition;

import com.hazelcast.nio.Address;

/**
 * @author mdogan 6/17/13
 */
public interface PartitionView {

    static final int MAX_REPLICA_COUNT = 7;
    static final int MAX_BACKUP_COUNT = MAX_REPLICA_COUNT - 1;

    int getPartitionId();

    Address getOwner();

    Address getReplicaAddress(int index);

    boolean isBackup(Address address);

    boolean isOwnerOrBackup(Address address);

    int getReplicaIndexOf(Address address);
}
