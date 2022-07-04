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

package com.hazelcast.splitbrainprotection.cache;

import com.hazelcast.cache.ICache;
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

import java.util.HashSet;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheSplitBrainProtectionReadTest extends AbstractSplitBrainProtectionTest {

    @Parameters(name = "splitBrainProtectionType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{SplitBrainProtectionOn.READ}, {SplitBrainProtectionOn.READ_WRITE}});
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
    public void get_splitBrainProtection() {
        cache(0).get(1);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void get_noSplitBrainProtection() {
        cache(3).get(1);
    }

    @Test
    public void getAsync_splitBrainProtection() throws Exception {
        cache(0).getAsync(1).toCompletableFuture().get();
    }

    @Test(expected = ExecutionException.class)
    public void getAsync_noSplitBrainProtection() throws Exception {
        cache(3).getAsync(1).toCompletableFuture().get();
    }

    @Test
    public void containsKey_splitBrainProtection() {
        cache(0).containsKey(1);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void containsKey_noSplitBrainProtection() {
        cache(3).containsKey(1);
    }

    @Test
    public void getAll_splitBrainProtection() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        cache(0).getAll(hashSet);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAll_noSplitBrainProtection() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        cache(3).getAll(hashSet);
    }

    @Test
    public void iterator_splitBrainProtection() {
        cache(0).iterator();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void iterator_noSplitBrainProtection() {
        cache(3).iterator();
    }

    @Test
    public void size_splitBrainProtection() {
        cache(0).size();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void size_noSplitBrainProtection() {
        cache(3).size();
    }

    @Test
    public void isDestroyed_splitBrainProtection() {
        cache(0).isDestroyed();
    }

    @Test
    public void isDestroyed_noSplitBrainProtection() {
        cache(3).isDestroyed();
    }

    @Test
    public void isClosed_splitBrainProtection() {
        cache(0).isClosed();
    }

    @Test
    public void isClosed_noSplitBrainProtection() {
        cache(3).isClosed();
    }

    @Test
    public void getLocalCacheStatistics_splitBrainProtection() {
        try {
            cache(0).getLocalCacheStatistics();
        } catch (UnsupportedOperationException ex) {
            // client workaround
        }
    }

    @Test
    public void getLocalCacheStatistics_noSplitBrainProtection() {
        try {
            cache(3).getLocalCacheStatistics();
        } catch (UnsupportedOperationException ex) {
            // client workaround
        }
    }

    protected ICache<Integer, String> cache(int index) {
        return cache(index, splitBrainProtectionOn);
    }
}
