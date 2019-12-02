/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.management.operation.RunScriptOperation;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
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
    public void testScriptingDisabled() throws InterruptedException, ExecutionException {
        testInternal(false, false);
    }

    @Test
    public void testScriptingDisabledOnSrc() throws InterruptedException, ExecutionException {
        testInternal(false, true);
    }

    @Test
    public void testScriptingDisabledOnDest() throws InterruptedException, ExecutionException {
        testInternal(true, false);
    }

    @Test
    public void testScriptingEnabled() throws InterruptedException, ExecutionException {
        testInternal(true, true);
    }

    @Test
    public void testDefaultValue() throws InterruptedException, ExecutionException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        RunScriptOperation op = createScriptExecutorOp();
        InternalCompletableFuture<Object> result = getOperationService(hz1).invokeOnTarget(MapService.SERVICE_NAME, op,
                getAddress(hz2));
        if (!getScriptingEnabledDefaultValue()) {
            expectedException.expect(ExecutionException.class);
            expectedException.expectCause(CoreMatchers.instanceOf(AccessControlException.class));
        }
        assertEquals(SCRIPT_RETURN_VAL, result.get());
    }

    /**
     * @return true if the scripting should be enabled by default
     */
    protected boolean getScriptingEnabledDefaultValue() {
        return true;
    }

    /**
     * Tests scripting protection on 2 nodes cluster. The source node sends a {@link RunScriptOperation} to the destination
     * one. If the destination node has scripting disabled, an exception is thrown, otherwise the source gets correct script
     * execution result.
     *
     * @param srcEnabled scripting enabled on source node (it's value should have no effect on the test)
     * @param destEnabled scripting enabled on destination node
     */
    protected void testInternal(boolean srcEnabled, boolean destEnabled) throws InterruptedException, ExecutionException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(createConfig(srcEnabled));
        HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig(destEnabled));
        RunScriptOperation op = createScriptExecutorOp();
        InternalCompletableFuture<Object> result = getOperationService(hz1).invokeOnTarget(MapService.SERVICE_NAME, op,
                getAddress(hz2));
        if (!destEnabled) {
            expectedException.expect(ExecutionException.class);
            expectedException.expectCause(CoreMatchers.<Throwable>instanceOf(AccessControlException.class));
        }
        assertEquals(SCRIPT_RETURN_VAL, result.get());
    }

    protected RunScriptOperation createScriptExecutorOp() {
        return new RunScriptOperation(ENGINE, SCRIPT);
    }

    protected Config createConfig(boolean scriptingEnabled) {
        Config config = new Config();
        config.getManagementCenterConfig().setScriptingEnabled(scriptingEnabled);
        return config;
    }
}
