/*
 * Copyright 2025 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kafka.connect.impl.processorsupplier;

import com.hazelcast.cluster.Address;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class TaskMaxProcessorMetaSupplierTest {
    private static final Address[] ADDRESSES = new Address[] {
            new Address(getLocalHost(), 5000),
            new Address(getLocalHost(), 5001),
            new Address(getLocalHost(), 5002),
            new Address(getLocalHost(), 5003),
            new Address(getLocalHost(), 5004),
    };

    private static InetAddress getLocalHost() {
        try {
            return InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private static Stream<Arguments> localParallelismTestData()  {
        return Stream.of(
                // 1 cluster member
                Arguments.of(
                        List.of(ADDRESSES[0]),
                        1,
                        // Number of processors per member
                        List.of(0)
                ),
                // 2 cluster members, localParallelism = 1
                Arguments.of(
                        List.of(ADDRESSES[0], ADDRESSES[1]),
                        1,
                        List.of(0, 1)
                ),
                // 2 cluster members, localParallelism = 2
                Arguments.of(
                        List.of(ADDRESSES[0], ADDRESSES[1]),
                        2,
                        List.of(0, 2)
                ),
                // 3 cluster members, localParallelism = 1
                Arguments.of(
                        List.of(ADDRESSES[0], ADDRESSES[1], ADDRESSES[2]),
                        1,
                        // Number of processors per member
                        List.of(0, 1, 2)
                ),
                // 3 cluster members, localParallelism = 2
                Arguments.of(
                        List.of(ADDRESSES[0], ADDRESSES[1], ADDRESSES[2]),
                        2,
                        // Number of processors per member
                        List.of(0, 2)
                )
        );
    }

    @ParameterizedTest
    @MethodSource("localParallelismTestData")
    void testGetOneMember(List<Address> addressList, int localParallelism, List<Integer> startingOrder) {

        TaskMaxProcessorMetaSupplier taskMaxProcessorMetaSupplier = new TaskMaxProcessorMetaSupplier();
        taskMaxProcessorMetaSupplier.setTasksMax(4);
        taskMaxProcessorMetaSupplier.setLocalParallelism(localParallelism);
        taskMaxProcessorMetaSupplier.get(addressList);

        assertThat(taskMaxProcessorMetaSupplier.isPartitionedAddresses()).isTrue();
        assertThat(taskMaxProcessorMetaSupplier.getStartingProcessorOrderMap().values())
                .containsExactlyInAnyOrderElementsOf(startingOrder);
    }
}
