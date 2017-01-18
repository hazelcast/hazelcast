package com.hazelcast.map.impl.loader;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.loader.KeyLoader.Role;
import static com.hazelcast.map.impl.loader.KeyLoader.Role.NONE;
import static com.hazelcast.map.impl.loader.KeyLoader.Role.RECEIVER;
import static com.hazelcast.map.impl.loader.KeyLoader.Role.SENDER;
import static com.hazelcast.map.impl.loader.KeyLoader.Role.SENDER_BACKUP;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@SuppressWarnings("ConstantConditions")
public class KeyLoaderUtilTest {

    @Test
    public void assignRole_SENDER() {
        boolean isPartitionOwner = true;
        boolean isMapNamePartition = true;
        boolean isMapNamePartitionFirstReplica = false;

        Role role = KeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, isMapNamePartitionFirstReplica);

        assertEquals(SENDER, role);
    }

    @Test
    public void assignRole_SENDER_BACKUP() {
        boolean isPartitionOwner = false;
        boolean isMapNamePartition = true;
        boolean isMapNamePartitionFirstReplica = true;

        Role role = KeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, isMapNamePartitionFirstReplica);

        assertEquals(SENDER_BACKUP, role);
    }

    @Test
    public void assignRole_NOT_SENDER_BACKUP() {
        boolean isPartitionOwner = false;
        boolean isMapNamePartition = true;
        boolean isMapNamePartitionFirstReplica = false;

        Role role = KeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, isMapNamePartitionFirstReplica);

        assertEquals(NONE, role);
    }

    @Test
    public void assignRole_RECEIVER_insignificantFlagFalse() {
        boolean isPartitionOwner = true;
        boolean isMapNamePartition = false;
        boolean insignificant = false;

        Role role = KeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, insignificant);

        assertEquals(RECEIVER, role);
    }

    @Test
    public void assignRole_RECEIVER_insignificantFlagTrue() {
        boolean isPartitionOwner = true;
        boolean isMapNamePartition = false;
        boolean insignificant = true;

        Role role = KeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, insignificant);

        assertEquals(RECEIVER, role);
    }

    @Test
    public void assignRole_NONE_insignificantFlagFalse() {
        boolean isPartitionOwner = false;
        boolean isMapNamePartition = false;
        boolean insignificant = false;

        Role role = KeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, insignificant);

        assertEquals(NONE, role);
    }

    @Test
    public void assignRole_NONE_insignificantFlagTrue() {
        boolean isPartitionOwner = false;
        boolean isMapNamePartition = false;
        boolean insignificant = true;

        Role role = KeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, insignificant);

        assertEquals(NONE, role);
    }

    @Test
    public void assignRole_NONE_impossibleCombination() {
        boolean isPartitionOwner = false;
        boolean isMapNamePartition = false;
        boolean insignificant = true;

        Role role = KeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, insignificant);

        assertEquals(NONE, role);
    }
}
