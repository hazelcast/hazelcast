/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_TABLE_SEND_INTERVAL;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Collections.singletonList;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_InitInvocationTargetTest extends HazelcastTestSupport {

    @Test
    public void testPartitionTableIsFetchedLazilyOnPartitionInvocation() throws ExecutionException, InterruptedException {
        Config config = new Config();
        config.setProperty(PARTITION_TABLE_SEND_INTERVAL.getName(), String.valueOf(Integer.MAX_VALUE));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances(config);

        dropOperationsBetween(instances[0], instances[1], PartitionDataSerializerHook.F_ID,
                singletonList(PartitionDataSerializerHook.PARTITION_STATE_OP));

        OperationServiceImpl operationService = getNodeEngineImpl(instances[1]).getOperationService();
        Future<Object> future = operationService.invokeOnPartition(null, new DummyOperation(), 0);

        resetPacketFiltersFrom(instances[0]);

        future.get();
    }
}
