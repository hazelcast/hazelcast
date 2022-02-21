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

package com.hazelcast.splitbrainprotection.multimap;

import com.hazelcast.multimap.MultiMap;
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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static java.util.Arrays.asList;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiMapSplitBrainProtectionReadTest extends AbstractSplitBrainProtectionTest {

    @Parameters(name = "splitBrainProtectionType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{SplitBrainProtectionOn.READ}, {SplitBrainProtectionOn.READ_WRITE}});
    }

    @Parameter
    public static SplitBrainProtectionOn splitBrainProtectionOn;

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(smallInstanceConfig(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void valueCount_successful_whenSplitBrainProtectionSize_met() {
        map(0).valueCount("foo");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void valueCount_failing_whenSplitBrainProtectionSize_notMet() {
        map(3).valueCount("foo");
    }

    @Test
    public void get_successful_whenSplitBrainProtectionSize_met() {
        map(0).get("foo");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void get_failing_whenSplitBrainProtectionSize_notMet() {
        map(3).get("foo");
    }

    @Test
    public void containsKey_successful_whenSplitBrainProtectionSize_met() {
        map(0).containsKey("foo");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void containsKey_failing_whenSplitBrainProtectionSize_notMet() {
        map(3).containsKey("foo");
    }

    @Test
    public void containsEntry_successful_whenSplitBrainProtectionSize_met() {
        map(0).containsEntry("foo", "bar");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void containsEntry_failing_whenSplitBrainProtectionSize_notMet() {
        map(3).containsEntry("foo", "bar");
    }

    @Test
    public void containsValue_successful_whenSplitBrainProtectionSize_met() {
        map(0).containsValue("foo");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void containsValue_failing_whenSplitBrainProtectionSize_notMet() {
        map(3).containsValue("foo");
    }

    @Test
    public void keySet_successful_whenSplitBrainProtectionSize_met() {
        map(0).keySet();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void keySet_failing_whenSplitBrainProtectionSize_notMet() {
        map(3).keySet();
    }

    @Test
    public void localKeySet_successful_whenSplitBrainProtectionSize_met() {
        try {
            map(0).localKeySet();
        } catch (UnsupportedOperationException ignored) {
        }
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void localKeySet_successful_whenSplitBrainProtectionSize_notMet() {
        try {
            map(3).localKeySet();
        } catch (UnsupportedOperationException ex) {
            throw new SplitBrainProtectionException("work-around for client side tests");
        }
    }

    @Test
    public void values_successful_whenSplitBrainProtectionSize_met() {
        map(0).values();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void values_failing_whenSplitBrainProtectionSize_notMet() {
        map(3).values();
    }

    @Test
    public void entrySet_successful_whenSplitBrainProtectionSize_met() {
        map(0).entrySet();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void entrySet_failing_whenSplitBrainProtectionSize_notMet() {
        map(3).entrySet();
    }

    @Test
    public void size_successful_whenSplitBrainProtectionSize_met() {
        map(0).size();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void size_failing_whenSplitBrainProtectionSize_notMet() {
        map(3).size();
    }

    @Test
    public void isLocked_successful_whenSplitBrainProtectionSize_met() {
        map(0).isLocked("foo");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void isLocked_failing_whenSplitBrainProtectionSize_notMet() {
        map(3).isLocked("foo");
    }

    protected MultiMap map(int index) {
        return multimap(index, splitBrainProtectionOn);
    }
}
