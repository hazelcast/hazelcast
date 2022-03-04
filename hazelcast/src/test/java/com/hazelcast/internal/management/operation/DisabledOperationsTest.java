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

package com.hazelcast.internal.management.operation;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCRunConsoleCommandCodec;
import com.hazelcast.client.impl.protocol.codec.MCRunScriptCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.security.AccessControlException;

import static com.hazelcast.test.HazelcastTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DisabledOperationsTest {

    private HazelcastClientInstanceImpl client;
    private TestHazelcastFactory factory;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory();

        // scripting and console are disabled by default
        factory.newHazelcastInstance(smallInstanceConfig());
        client = ((HazelcastClientProxy) factory.newHazelcastClient()).client;
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testRunConsoleCommandIsNotAllowed() throws Exception {
        ClientMessage clientMessage = MCRunConsoleCommandCodec.encodeRequest(randomString(), randomString());
        assertFailure(clientMessage, "Using Console is not allowed on this Hazelcast member.");
    }

    @Test
    public void testRunScriptIsNotAllowed() throws Exception {
        ClientMessage clientMessage = MCRunScriptCodec.encodeRequest(randomString(), randomString());
        assertFailure(clientMessage, "Using ScriptEngine is not allowed on this Hazelcast member.");
    }

    private void assertFailure(ClientMessage clientMessage, String expectedExceptionMsg) throws Exception {
        ClientInvocation invocation = new ClientInvocation(client, clientMessage, null);
        ClientInvocationFuture future = invocation.invoke();
        assertThatThrownBy(() -> future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS))
                .hasCauseInstanceOf(AccessControlException.class)
                .hasRootCauseMessage(expectedExceptionMsg);
    }

}
