/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management;

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.codec.MCRunScriptCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.security.AccessControlException;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

/**
 * Tests possibility to disable scripting on members.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class ScriptingProtectionTest extends HazelcastTestSupport {

    private static final String SCRIPT_RETURN_VAL = "John";
    private static final String SCRIPT = "\"" + SCRIPT_RETURN_VAL + "\"";
    // Let's use Groovy on classpath, because Azul Zulu 6-7 doesn't include the JavaScript engine (Rhino)
    private static final String ENGINE = "groovy";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    @AfterClass
    public static void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testScriptingDisabled() throws Exception {
        testInternal(false);
    }

    @Test
    public void testScriptingEnabled() throws Exception {
        testInternal(true);
    }

    @Test
    public void testDefaultValue() throws Exception {
        testInternal(null, false);
    }

    /**
     * Tests scripting protection on single node cluster with a client. The client tries to run script on the node.
     * If the node has scripting disabled, an exception is thrown, otherwise the client gets correct script
     * execution result.
     *
     * @param enabled scripting enabled on the node
     */
    protected void testInternal(boolean enabled) throws Exception {
        testInternal(createConfig(enabled), enabled);
    }

    private void testInternal(Config config, boolean expectEnabled) throws Exception {
        TestHazelcastFactory factory = new TestHazelcastFactory(1);

        try {
            HazelcastInstance hz = config != null ? factory.newHazelcastInstance(config) : factory.newHazelcastInstance();
            if (!expectEnabled) {
                expectedException.expect(ExecutionException.class);
                expectedException.expectCause(CoreMatchers.instanceOf(AccessControlException.class));
            }
            HazelcastInstance client = factory.newHazelcastClient();
            Object result = runScript(
                    client,
                    hz.getCluster().getLocalMember()).get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
            assertEquals(SCRIPT_RETURN_VAL, result);
        } finally {
            factory.shutdownAll();
        }
    }

    protected Config createConfig(boolean scriptingEnabled) {
        Config config = new Config();
        config.getManagementCenterConfig().setScriptingEnabled(scriptingEnabled);
        return config;
    }

    private ClientDelegatingFuture<Object> runScript(HazelcastInstance client, Member member) {
        ClientInvocation invocation = new ClientInvocation(
                ((HazelcastClientProxy) client).client,
                MCRunScriptCodec.encodeRequest(ENGINE, SCRIPT),
                null,
                member.getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                ((HazelcastClientProxy) client).client.getSerializationService(),
                MCRunScriptCodec::decodeResponse
        );
    }
}
