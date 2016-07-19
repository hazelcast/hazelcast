package com.hazelcast.map.impl;

import org.junit.Test;

import static com.hazelcast.map.impl.MapKeyLoader.Role;
import static com.hazelcast.map.impl.MapKeyLoader.Role.NONE;
import static com.hazelcast.map.impl.MapKeyLoader.Role.RECEIVER;
import static com.hazelcast.map.impl.MapKeyLoader.Role.SENDER;
import static com.hazelcast.map.impl.MapKeyLoader.Role.SENDER_BACKUP;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ConstantConditions")
public class MapKeyLoaderUtilTest {

    @Test
    public void assignRole_SENDER() {
        boolean isPartitionOwner = true;
        boolean isMapNamePartition = true;
        boolean isMapNamePartitionFirstReplica = false;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, isMapNamePartitionFirstReplica);

        assertEquals(SENDER, role);
    }

    @Test
    public void assignRole_SENDER_BACKUP() {
        boolean isPartitionOwner = false;
        boolean isMapNamePartition = true;
        boolean isMapNamePartitionFirstReplica = true;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, isMapNamePartitionFirstReplica);

        assertEquals(SENDER_BACKUP, role);
    }

    @Test
    public void assignRole_NOT_SENDER_BACKUP() {
        boolean isPartitionOwner = false;
        boolean isMapNamePartition = true;
        boolean isMapNamePartitionFirstReplica = false;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, isMapNamePartitionFirstReplica);

        assertEquals(NONE, role);
    }

    @Test
    public void assignRole_RECEIVER_insignificantFlagFalse() {
        boolean isPartitionOwner = true;
        boolean isMapNamePartition = false;
        boolean insignificant = false;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, insignificant);

        assertEquals(RECEIVER, role);
    }

    @Test
    public void assignRole_RECEIVER_insignificantFlagTrue() {
        boolean isPartitionOwner = true;
        boolean isMapNamePartition = false;
        boolean insignificant = true;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, insignificant);

        assertEquals(RECEIVER, role);
    }

    @Test
    public void assignRole_NONE_insignificantFlagFalse() {
        boolean isPartitionOwner = false;
        boolean isMapNamePartition = false;
        boolean insignificant = false;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, insignificant);

        assertEquals(NONE, role);
    }

    @Test
    public void assignRole_NONE_insignificantFlagTrue() {
        boolean isPartitionOwner = false;
        boolean isMapNamePartition = false;
        boolean insignificant = true;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, insignificant);

        assertEquals(NONE, role);
    }

    @Test
    public void assignRole_NONE_impossibleCombination() {
        boolean isPartitionOwner = false;
        boolean isMapNamePartition = false;
        boolean insignificant = true;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, insignificant);

        assertEquals(NONE, role);
    }
}
