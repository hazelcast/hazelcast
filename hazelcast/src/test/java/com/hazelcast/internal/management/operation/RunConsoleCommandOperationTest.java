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

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.codec.MCRunConsoleCommandCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RunConsoleCommandOperationTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private TestHazelcastFactory factory;
    private HazelcastClientInstanceImpl client;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory();

        Config config = smallInstanceConfig();
        config.getManagementCenterConfig().setConsoleEnabled(true);
        hz = factory.newHazelcastInstance(config);
        client = ((HazelcastClientProxy) factory.newHazelcastClient()).client;
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testRunConsoleCommand() throws Exception {
        String actual = runCommand(client, hz).get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertThat(actual).contains("Members [1]");
    }

    private ClientDelegatingFuture<String> runCommand(HazelcastClientInstanceImpl client, HazelcastInstance member) {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCRunConsoleCommandCodec.encodeRequest(null, "who"),
                null,
                member.getCluster().getLocalMember().getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                client.getSerializationService(),
                MCRunConsoleCommandCodec::decodeResponse
        );
    }
}
