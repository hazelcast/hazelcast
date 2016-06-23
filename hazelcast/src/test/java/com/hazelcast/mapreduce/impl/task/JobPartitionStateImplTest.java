package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.mapreduce.JobPartitionState.State.CANCELLED;
import static com.hazelcast.mapreduce.JobPartitionState.State.WAITING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class JobPartitionStateImplTest {

    private Address address;
    private Address otherAddress;

    private JobPartitionStateImpl jobPartitionState;
    private JobPartitionStateImpl jobPartitionStateSameAttributes;

    private JobPartitionStateImpl jobPartitionStateOtherAddress;
    private JobPartitionStateImpl jobPartitionStateOtherState;

    @Before
    public void setUp() throws Exception {
        address = new Address("127.0.0.1", 5701);
        otherAddress = new Address("127.0.0.1", 5702);

        jobPartitionState = new JobPartitionStateImpl(address, WAITING);
        jobPartitionStateSameAttributes = new JobPartitionStateImpl(address, WAITING);

        jobPartitionStateOtherAddress = new JobPartitionStateImpl(otherAddress, WAITING);
        jobPartitionStateOtherState = new JobPartitionStateImpl(address, CANCELLED);
    }

    @Test
    public void testGetOwner() {
        assertEquals(address, jobPartitionState.getOwner());
        assertEquals(otherAddress, jobPartitionStateOtherAddress.getOwner());
    }

    @Test
    public void testGetState() {
        assertEquals(WAITING, jobPartitionState.getState());
        assertEquals(CANCELLED, jobPartitionStateOtherState.getState());
    }

    @Test
    public void testToString() {
        assertNotNull(jobPartitionState.toString());
    }

    @Test
    public void testEquals() {
        assertEquals(jobPartitionState, jobPartitionState);
        assertEquals(jobPartitionState, jobPartitionStateSameAttributes);

        assertNotEquals(jobPartitionState, null);
        assertNotEquals(jobPartitionState, new Object());

        assertNotEquals(jobPartitionState, jobPartitionStateOtherAddress);
        assertNotEquals(jobPartitionState, jobPartitionStateOtherState);
    }

    @Test
    public void testHashCode() {
        assertEquals(jobPartitionState.hashCode(), jobPartitionState.hashCode());
        assertEquals(jobPartitionState.hashCode(), jobPartitionStateSameAttributes.hashCode());

        assertNotEquals(jobPartitionState.hashCode(), jobPartitionStateOtherAddress.hashCode());
        assertNotEquals(jobPartitionState.hashCode(), jobPartitionStateOtherState.hashCode());
    }
}
