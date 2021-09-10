/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql;

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.codec.SqlMappingDdlCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNull;

public class MCMessageTasksTest extends SqlTestSupport {

    @BeforeClass
    public static void setUpClass() {
        initializeWithClient(1, null, null);
    }

    @Test
    public void test_sqlMappingDdl_nonExistingMap() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                SqlMappingDdlCodec.encodeRequest(randomMapName()),
                null
        );

        ClientDelegatingFuture<String> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                SqlMappingDdlCodec::decodeResponse
        );

        String response = future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertNull(response);
    }

    @Test
    public void test_sqlMappingDdl_existingMap() throws Exception {
        String name = randomMapName();
        instance().getMap(name).put(1, "value-1");

        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                SqlMappingDdlCodec.encodeRequest(name),
                null
        );

        ClientDelegatingFuture<String> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                SqlMappingDdlCodec::decodeResponse
        );

        String response = future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertStartsWith("CREATE MAPPING \"" + name + "\"", response);
    }

    private HazelcastClientInstanceImpl getClientImpl() {
        return ((HazelcastClientProxy) client()).client;
    }
}
