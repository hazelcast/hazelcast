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

package com.hazelcast.splitbrainprotection.list;

import com.hazelcast.collection.IList;
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

import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.READ_WRITE;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.WRITE;
import static java.util.Arrays.asList;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ListSplitBrainProtectionWriteTest extends AbstractSplitBrainProtectionTest {

    @Parameters(name = "splitBrainProtectionType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{WRITE}, {READ_WRITE}});
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
    public void addOperation_successful_whenSplitBrainProtectionSize_met() {
        list(0).add("foo");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void addOperation_successful_whenSplitBrainProtectionSize_notMet() {
        list(3).add("foo");
    }

    @Test
    public void addAllOperation_successful_whenSplitBrainProtectionSize_met() {
        list(0).addAll(asList("foo", "bar"));
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void addAllOperation_successful_whenSplitBrainProtectionSize_notMet() {
        list(3).add(asList("foo", "bar"));
    }

    @Test
    public void removeOperation_successful_whenSplitBrainProtectionSize_met() {
        list(0).remove("foo");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void removeOperation_successful_whenSplitBrainProtectionSize_notMet() {
        list(3).remove("foo");
    }

    @Test
    public void compareAndRemoveOperation_removeAll_successful_whenSplitBrainProtectionSize_met() {
        list(0).removeAll(asList("foo", "bar"));
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void compareAndRemoveOperation_removeAll_successful_whenSplitBrainProtectionSize_notMet() {
        list(3).removeAll(asList("foo", "bar"));
    }

    @Test
    public void compareAndRemoveOperation_retainAll_successful_whenSplitBrainProtectionSize_met() {
        list(0).removeAll(asList("foo", "bar"));
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void compareAndRemoveOperation_retainAll_successful_whenSplitBrainProtectionSize_notMet() {
        list(3).removeAll(asList("foo", "bar"));
    }

    @Test
    public void clearOperation_successful_whenSplitBrainProtectionSize_met() {
        list(0).clear();
        list(0).add("object123");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void clearOperation_successful_whenSplitBrainProtectionSize_notMet() {
        list(3).clear();
    }

    @Test
    public void setOperation_successful_whenSplitBrainProtectionSize_met() {
        try {
            list(0).set(0, "bar");
        } catch (IndexOutOfBoundsException ex) {
            // meaningless
        }
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void setOperation_successful_whenSplitBrainProtectionSize_notMet() {
        list(3).set(0, "bar");
    }

    protected IList list(int index) {
        return list(index, splitBrainProtectionOn);
    }
}
