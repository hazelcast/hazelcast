/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.protocol.task;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddVectorCollectionConfigCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionClearCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionDeleteCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionGetCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionOptimizeCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionPutAllCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionPutCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionPutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionSearchNearVectorCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionSetCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionSizeCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.vector.impl.DataVectorDocument;
import com.hazelcast.vector.impl.SearchOptionsImpl;
import com.hazelcast.vector.impl.SingleIndexVectorValues;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import static com.hazelcast.vector.impl.spi.VectorCollectionLocator.MISSED_VECTOR_MODULE_MESSAGE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatCode;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorMessageTaskServiceNotFoundTest extends HazelcastTestSupport {

    private HazelcastInstance client;
    private TestHazelcastFactory factory;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory();
        factory.newHazelcastInstance(smallInstanceConfig());
        client = factory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testAddVectorCollectionConfigMessageTask() {
        assertException(
            DynamicConfigAddVectorCollectionConfigCodec.encodeRequest(
                "vector-collection",
                Collections.emptyList(),
                1,
                0,
                null,
                "DiscardMergePolicy",
                100,
                null
            )
        );
    }

    @Test
    public void testVectorCollectionSizeMessageTask() {
        assertException(
            VectorCollectionSizeCodec.encodeRequest("vector-collection")
        );
    }

    @Test
    public void testVectorCollectionGetMessageTask() {
        assertException(
            VectorCollectionGetCodec.encodeRequest(
                "vector-collection",
                new HeapData(new byte[]{1, 2, 3, 4, 5, 6, 7, 8})
            )
        );
    }

    @Test
    public void testVectorCollectionPutMessageTask() {
        var data = new HeapData(new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        assertException(
            VectorCollectionPutCodec.encodeRequest(
                "vector-collection",
                data,
                new DataVectorDocument(data, new SingleIndexVectorValues(new float[]{1.0f, 2.0f, 3.0f}))
            )
        );
    }

    @Test
    public void testVectorCollectionPutIfAbsentMessageTask() {
        var data = new HeapData(new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        assertException(
            VectorCollectionPutIfAbsentCodec.encodeRequest(
                "vector-collection",
                data,
                new DataVectorDocument(data, new SingleIndexVectorValues(new float[]{1.0f, 2.0f, 3.0f}))
            )
        );
    }

    @Test
    public void testVectorCollectionRemoveMessageTask() {
        assertException(
            VectorCollectionRemoveCodec.encodeRequest(
                "vector-collection",
                new HeapData(new byte[]{1, 2, 3, 4, 5, 6, 7, 8})
            )
        );
    }

    @Test
    public void testVectorCollectionDeleteMessageTask() {
        assertException(
            VectorCollectionDeleteCodec.encodeRequest(
                "vector-collection",
                new HeapData(new byte[]{1, 2, 3, 4, 5, 6, 7, 8})
            )
        );
    }

    @Test
    public void testVectorCollectionPutAllMessageTask() {
        assertException(
            VectorCollectionPutAllCodec.encodeRequest(
                "vector-collection",
                new ArrayList<>()
            )
        );
    }

    @Test
    public void testVectorCollectionSetMessageTask() {
        var data = new HeapData(new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        assertException(
            VectorCollectionSetCodec.encodeRequest(
                "vector-collection",
                data,
                new DataVectorDocument(data, new SingleIndexVectorValues(new float[]{1.0f, 2.0f, 3.0f}))
            )
        );
    }

    @Test
    public void testVectorCollectionSearchNearVectorMessageTask() {
        assertException(
            VectorCollectionSearchNearVectorCodec.encodeRequest(
                "vector-collection",
                new SingleIndexVectorValues(new float[]{1.0f, 2.0f, 3.0f}),
                new SearchOptionsImpl()
            )
        );
    }

    @Test
    public void testVectorCollectionOptimizeMessageTask() {
        assertException(
            VectorCollectionOptimizeCodec.encodeRequest("vector-collection", "index", UUID.randomUUID())
        );
    }

    @Test
    public void testVectorCollectionClearMessageTask() {
        assertException(
            VectorCollectionClearCodec.encodeRequest("vector-collection")
        );
    }

    private HazelcastClientInstanceImpl getClientImpl() {
        return ((HazelcastClientProxy) client).client;
    }

    private void assertException(ClientMessage clientMessage) {
        ClientInvocation invocation = new ClientInvocation(
            getClientImpl(),
            clientMessage,
            null
        );
        ClientInvocationFuture future = invocation.invoke();
        assertThatCode(() -> future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS))
            .hasCauseInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining(MISSED_VECTOR_MODULE_MESSAGE);
    }
}
