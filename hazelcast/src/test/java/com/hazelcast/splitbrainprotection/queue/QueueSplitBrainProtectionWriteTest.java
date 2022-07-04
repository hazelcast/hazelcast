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

package com.hazelcast.splitbrainprotection.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionTest;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.READ_WRITE;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.WRITE;
import static java.util.Arrays.asList;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueSplitBrainProtectionWriteTest extends AbstractSplitBrainProtectionTest {

    @Parameters(name = "splitBrainProtectionType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{WRITE}, {READ_WRITE}});
    }

    @Parameter
    public static SplitBrainProtectionOn splitBrainProtectionOn;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(smallInstanceConfig(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void put_splitBrainProtection() throws Exception {
        queue(0).put("foo");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void put_noSplitBrainProtection() throws Exception {
        queue(3).put("foo");
    }

    @Test
    public void offer_splitBrainProtection() {
        queue(0).offer("foo");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void offer_noSplitBrainProtection() {
        queue(3).offer("foo");
    }

    @Test
    public void add_splitBrainProtection() {
        queue(0).add("foo");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void add_noSplitBrainProtection() {
        queue(3).add("foo");
    }

    @Test
    public void take_splitBrainProtection() throws Exception {
        queue(0).take();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void take_noSplitBrainProtection() throws Exception {
        queue(3).take();
    }

    @Test
    public void remove_splitBrainProtection() {
        queue(0).remove();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void remove_noSplitBrainProtection() {
        queue(3).remove();
    }

    @Test
    public void poll_splitBrainProtection() {
        queue(0).poll();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void poll_noSplitBrainProtection() {
        queue(3).poll();
    }

    protected IQueue queue(int index) {
        return queue(index, splitBrainProtectionOn);
    }
}
