/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.partition;

import com.hazelcast.internal.partition.impl.DummyInternalPartition;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Collections;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitionRuntimeStateTest extends HazelcastTestSupport {

    @Test
    public void toString_whenConstructed() throws UnknownHostException {
        PartitionRuntimeState state = createPartitionState(0,
                address("127.0.0.1", 5701),
                address("127.0.0.2", 5702)
        );
        assertContains(state.toString(), "127.0.0.1");
        assertContains(state.toString(), "127.0.0.2");
    }

    @Test
    public void toString_whenDeserialized() throws UnknownHostException {
        PartitionRuntimeState state = createPartitionState(0,
                address("127.0.0.1", 5701),
                address("127.0.0.2", 5702)
        );

        state = serializeAndDeserialize(state);
        assertContains(state.toString(), "127.0.0.1");
        assertContains(state.toString(), "127.0.0.2");
    }

    @Test
    public void toString_whenDeserializedTwice() throws UnknownHostException {
        PartitionRuntimeState state = createPartitionState(0,
                address("127.0.0.1", 5701),
                address("127.0.0.2", 5702)
        );

        state = serializeAndDeserialize(state);
        state = serializeAndDeserialize(state);
        assertContains(state.toString(), "127.0.0.1");
        assertContains(state.toString(), "127.0.0.2");
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

    private PartitionRuntimeState createPartitionState(int partitionId, Address... addresss) throws UnknownHostException {
        DummyInternalPartition partition = new DummyInternalPartition(addresss, partitionId);
        return new PartitionRuntimeState(new InternalPartition[]{partition}, Collections.<MigrationInfo>emptyList(), partitionId);
    }

    private Address address(String host, int port) throws UnknownHostException {
        return new Address(host, port);
    }
}
