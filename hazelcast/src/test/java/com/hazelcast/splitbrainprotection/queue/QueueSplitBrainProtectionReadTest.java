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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
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

import static java.util.Arrays.asList;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueSplitBrainProtectionReadTest extends AbstractSplitBrainProtectionTest {

    private static final ILogger LOGGER = Logger.getLogger(QueueSplitBrainProtectionReadTest.class);

    @Parameters(name = "splitBrainProtectionType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][] {{SplitBrainProtectionOn.READ}, {SplitBrainProtectionOn.READ_WRITE}});
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
    public void element_splitBrainProtection() {
        LOGGER.info(String.valueOf(queue(0).size()));
        LOGGER.info(String.valueOf(queue(0).getPartitionKey()));
        queue(0).element();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void element_noSplitBrainProtection() {
        queue(3).element();
    }

    @Test
    public void peek_splitBrainProtection() {
        queue(0).peek();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void peek_noSplitBrainProtection() {
        queue(3).peek();
    }

    @Test
    public void getLocalQueueStats_splitBrainProtection() {
        try {
            queue(0).getLocalQueueStats();
        } catch (UnsupportedOperationException ex) {
            // client workaround
        }
    }

    @Test
    public void getLocalQueueStats_noSplitBrainProtection() {
        try {
            queue(3).getLocalQueueStats();
        } catch (UnsupportedOperationException ex) {
            // client workaround
        }
    }

    protected IQueue queue(int index) {
        return queue(index, splitBrainProtectionOn);
    }
}
