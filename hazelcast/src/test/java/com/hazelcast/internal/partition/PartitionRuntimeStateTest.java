package com.hazelcast.internal.partition;

import com.hazelcast.internal.partition.impl.DummyInternalPartition;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Collections;

import static org.junit.Assert.assertTrue;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitionRuntimeStateTest {

    @Test
    public void toString_whenConstructed() throws UnknownHostException {
        PartitionRuntimeState state = createPartitionState(0,
                address("127.0.0.1", 5701),
                address("127.0.0.2", 5702)
        );
        assertTrue(state.toString().contains("127.0.0.1"));
        assertTrue(state.toString().contains("127.0.0.2"));
    }

    @Test
    public void toString_whenDeserialized() throws UnknownHostException {
        PartitionRuntimeState state = createPartitionState(0,
                address("127.0.0.1", 5701),
                address("127.0.0.2", 5702)
        );

        state = serializeAndDeserialize(state);
        assertTrue(state.toString().contains("127.0.0.1"));
        assertTrue(state.toString().contains("127.0.0.2"));
    }

    @Test
    public void toString_whenDeserializedTwice() throws UnknownHostException {
        PartitionRuntimeState state = createPartitionState(0,
                address("127.0.0.1", 5701),
                address("127.0.0.2", 5702)
        );

        state = serializeAndDeserialize(state);
        state = serializeAndDeserialize(state);
        assertTrue(state.toString().contains("127.0.0.1"));
        assertTrue(state.toString().contains("127.0.0.2"));
    }

    private PartitionRuntimeState serializeAndDeserialize(PartitionRuntimeState state) {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        try {
            Data data = serializationService.toData(state);
            state = serializationService.toObject(data);
        } finally {
            serializationService.dispose();
        }
        return state;
    }

    private PartitionRuntimeState createPartitionState(int partitionId, Address...addresss) throws UnknownHostException {
        DummyInternalPartition partition = new DummyInternalPartition(addresss, partitionId);
        return new PartitionRuntimeState(new InternalPartition[]{partition}, Collections.<MigrationInfo>emptyList(), partitionId);
    }

    private Address address(String host, int port) throws UnknownHostException {
        return new Address(host, port);
    }
}
