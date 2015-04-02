package com.hazelcast.partition.impl;

import com.hazelcast.spi.annotation.PrivateApi;

/**
 * Used to indicate the reason behind an update on the partition talbe
 */
@PrivateApi
enum PartitionReplicaChangeReason {

    /*
        Used for initial partition assignments and migrations
     */
    ASSIGNMENT,

    /*
        Used when a replica node is shifted up in the partition table
     */
    MEMBER_REMOVED

}
