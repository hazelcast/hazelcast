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

package com.hazelcast.splitbrainprotection.set;

import com.hazelcast.collection.ISet;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionTest;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SetSplitBrainProtectionReadTest extends AbstractSplitBrainProtectionTest {

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
    public void containsOperation_successful_whenSplitBrainProtectionSize_met() {
        set(0).contains("foo");
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void containsOperation_successful_whenSplitBrainProtectionSize_notMet() {
        set(3).contains("foo");
    }

    @Test
    public void containsAllOperation_successful_whenSplitBrainProtectionSize_met() {
        set(0).containsAll(asList("foo", "bar"));
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void containsAllOperation_successful_whenSplitBrainProtectionSize_notMet() {
        set(3).containsAll(asList("foo", "bar"));
    }

    @Test
    public void isEmptyOperation_successful_whenSplitBrainProtectionSize_met() {
        set(0).isEmpty();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void isEmptyOperation_successful_whenSplitBrainProtectionSize_notMet() {
        set(3).isEmpty();
    }

    @Test
    public void sizeOperation_successful_whenSplitBrainProtectionSize_met() {
        set(0).size();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void sizeOperation_successful_whenSplitBrainProtectionSize_notMet() {
        set(3).size();
    }

    @Test
    public void getAllOperation_toArray_successful_whenSplitBrainProtectionSize_met() {
        set(0).toArray();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAllOperation_toArray_successful_whenSplitBrainProtectionSize_notMet() {
        set(3).toArray();
    }

    @Test
    public void getAllOperation_toArrayT_successful_whenSplitBrainProtectionSize_met() {
        set(0).toArray(new Object[0]);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAllOperation_toArrayT_successful_whenSplitBrainProtectionSize_notMet() {
        set(3).toArray(new Object[0]);
    }

    @Test
    public void getAllOperation_iterator_successful_whenSplitBrainProtectionSize_met() {
        set(0).iterator();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAllOperation_iterator_successful_whenSplitBrainProtectionSize_notMet() {
        set(3).iterator();
    }

    protected ISet set(int index) {
        return set(index, splitBrainProtectionOn);
    }
}
