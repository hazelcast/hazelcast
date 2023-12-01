/*
 * Copyright 2023 Hazelcast Inc.
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

    private static Stream<Arguments> localParallelismTestData() throws UnknownHostException {
        return Stream.of(
                // 1 cluster member
                Arguments.of(
                        List.of(new Address(InetAddress.getLocalHost(), 5000)),
                        // Number of processors per member
                        List.of(4)
                ),
                // 2 cluster members
                Arguments.of(
                        List.of(new Address(InetAddress.getLocalHost(), 5000),
                                new Address(InetAddress.getLocalHost(), 5001)),
                        List.of(2, 2)
                ),
                // 3 cluster members
                Arguments.of(
                        List.of(new Address(InetAddress.getLocalHost(), 5000),
                                new Address(InetAddress.getLocalHost(), 5001),
                                new Address(InetAddress.getLocalHost(), 5002)
                        ),
                        // Number of processors per member
                        List.of(1, 1, 2)
                ),
                // 4 cluster members
                Arguments.of(
                        List.of(new Address(InetAddress.getLocalHost(), 5000),
                                new Address(InetAddress.getLocalHost(), 5001),
                                new Address(InetAddress.getLocalHost(), 5002),
                                new Address(InetAddress.getLocalHost(), 5003)
                        ),
                        // Number of processors per member
                        List.of(1, 1, 1, 1)
                ),
                // 5 cluster members
                Arguments.of(
                        List.of(new Address(InetAddress.getLocalHost(), 5000),
                                new Address(InetAddress.getLocalHost(), 5001),
                                new Address(InetAddress.getLocalHost(), 5002),
                                new Address(InetAddress.getLocalHost(), 5003),
                                new Address(InetAddress.getLocalHost(), 5004)
                        ),
                        // Number of processors per member
                        List.of(0, 0, 0, 0, 4)
                )
        );
    }

    @ParameterizedTest
    @MethodSource("localParallelismTestData")
    void testGetOneMember(List<Address> addressList,
                          List<Integer> memberLocalParallelismList) {

        TaskMaxProcessorMetaSupplier taskMaxProcessorMetaSupplier = new TaskMaxProcessorMetaSupplier();
        taskMaxProcessorMetaSupplier.setTasksMax(4);
        taskMaxProcessorMetaSupplier.get(addressList);

        assertThat(taskMaxProcessorMetaSupplier.isPartitionedAddresses()).isTrue();
        assertThat(taskMaxProcessorMetaSupplier.getMemberLocalParallelismList()).containsAll(memberLocalParallelismList);
    }
}
