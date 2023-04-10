/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static com.hazelcast.test.Accessors.getOperationService;
import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_ArbitraryArityConstructionTest extends HazelcastTestSupport {

    private static final Object RESPONSE = "someresponse";
    private final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

    private HazelcastInstance local;
    private HazelcastInstance remote;
    private InvocationFuture<Object> f1;
    private InvocationFuture<Object> f2;

    @Before
    public void setUp() throws Exception {
        long callTimeoutMillis = 200;
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeoutMillis);

        local = factory.newHazelcastInstance(config);
        remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        OperationService opService = getOperationService(local);

        // block the partition thread
        f1 = opService.invokeOnPartition(
                null,
                new SlowOperation(callTimeoutMillis * 2, RESPONSE),
                getPartitionId(remote)
        );

        // this future will timeout since partition thread is blocked
        f2 = opService.invokeOnPartition(
                null,
                new DummyOperation(),
                getPartitionId(remote)
        );
    }

    @Test
    public void testAllOf_whenTimeout() {
        CompletableFuture<Void> allOf = CompletableFuture.allOf(f1, f2);

        // should throw since f2 will time out
        assertThrows(CompletionException.class, f2::join);
        assertThrows(CompletionException.class, allOf::join);
    }

    @Test
    public void testAnyOf_whenSomeTimeout() {
        CompletableFuture<Object> anyOf = CompletableFuture.anyOf(f1, f2);

        // should not throw since f1 will complete
        assertEquals(RESPONSE, f1.join());
        assertThrows(CompletionException.class, f2::join);

        try {
            // response being in the serialized form is a limitation not a requirement
            assertEquals(RESPONSE, getSerializationService(local).<NormalResponse>toObject(anyOf.join()).getValue());
            // reaching here is normal, anyOf completed via f1
        } catch (CompletionException ignore) {
            // reaching here is also normal, anyOf completed via f2
        }
    }

    @Test
    public void testAnyOf_whenAllTimeout() {
        // this future will also time out since partition thread is blocked
        InvocationFuture<Object> f3 = getOperationService(local).invokeOnPartition(
                null,
                new DummyOperation(),
                getPartitionId(remote)
        );

        CompletableFuture<Object> anyOf = CompletableFuture.anyOf(f2, f3);

        // should throw since f2 and f3 will throw
        assertThrows(CompletionException.class, f2::join);
        assertThrows(CompletionException.class, f3::join);
        assertThrows(CompletionException.class, anyOf::join);
    }
}
