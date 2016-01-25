package com.hazelcast.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.PartitionLostListenerStressTest.EventCollectingPartitionLostListener;
import com.hazelcast.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitionLostListenerTest extends AbstractPartitionLostListenerTest {

    @Override
    public int getNodeCount() {
        return 2;
    }

    @Test
    public void test_partitionLostListenerInvoked() {
        List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp(1);
        HazelcastInstance instance = instances.get(0);

        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();
        instance.getPartitionService().addPartitionLostListener(listener);

        final InternalPartitionLostEvent internalEvent = new InternalPartitionLostEvent(1, 0, null);

        NodeEngineImpl nodeEngine = getNode(instance).getNodeEngine();
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        partitionService.onPartitionLost(internalEvent);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                List<PartitionLostEvent> events = listener.getEvents();
                assertEquals(1, events.size());

                PartitionLostEvent event = events.get(0);
                assertEquals(internalEvent.getPartitionId(), event.getPartitionId());
                assertEquals(internalEvent.getLostReplicaIndex(), event.getLostBackupCount());
            }
        });
    }

    @Test
    public void test_partitionLostListenerInvoked_whenNodeCrashed() {
        List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();

        HazelcastInstance survivingInstance = instances.get(0);
        HazelcastInstance terminatingInstance = instances.get(1);

        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();
        survivingInstance.getPartitionService().addPartitionLostListener(listener);

        Node survivingNode = getNode(survivingInstance);
        final Address survivingAddress = survivingNode.getThisAddress();

        final Set<Integer> survivingPartitionIds = new HashSet<Integer>();
        for (InternalPartition partition : survivingNode.getPartitionService().getPartitions()) {
            if (survivingAddress.equals(partition.getReplicaAddress(0))) {
                survivingPartitionIds.add(partition.getPartitionId());
            }
        }

        terminatingInstance.getLifecycleService().terminate();
        waitAllForSafeState(survivingInstance);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                List<PartitionLostEvent> events = listener.getEvents();
                assertFalse(events.isEmpty());

                for (PartitionLostEvent event : events) {
                    assertEquals(survivingAddress, event.getEventSource());
                    assertFalse(survivingPartitionIds.contains(event.getPartitionId()));
                    assertEquals(0, event.getLostBackupCount());
                }
            }
        });
    }

    @Test
    public void test_internalPartitionLostEvent_serialization() throws IOException {
        Address address = new Address();
        InternalPartitionLostEvent internalEvent = new InternalPartitionLostEvent(1, 2, address);

        ObjectDataOutput output = mock(ObjectDataOutput.class);
        internalEvent.writeData(output);

        verify(output).writeInt(1);
        verify(output).writeInt(2);
    }

    @Test
    public void test_internalPartitionLostEvent_deserialization() throws IOException {
        InternalPartitionLostEvent internalEvent = new InternalPartitionLostEvent();

        ObjectDataInput input = mock(ObjectDataInput.class);
        when(input.readInt()).thenReturn(1, 2);

        internalEvent.readData(input);

        assertEquals(1, internalEvent.getPartitionId());
        assertEquals(2, internalEvent.getLostReplicaIndex());
    }

    @Test
    public void test_internalPartitionLostEvent_toString() {
        assertNotNull(new InternalPartitionLostEvent().toString());
    }
}
