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

package com.hazelcast.quorum.cache;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.Config;
import com.hazelcast.quorum.AbstractQuorumTest;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.HashSet;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheQuorumReadTest extends AbstractQuorumTest {

    @Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{QuorumType.READ}, {QuorumType.READ_WRITE}});
    }

    @Parameter
    public static QuorumType quorumType;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(new Config(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void get_quorum() {
        cache(0).get(1);
    }

    @Test(expected = QuorumException.class)
    public void get_noQuorum() {
        cache(3).get(1);
    }

    @Test
    public void getAsync_quorum() throws Exception {
        cache(0).getAsync(1).get();
    }

    @Test(expected = ExecutionException.class)
    public void getAsync_noQuorum() throws Exception {
        cache(3).getAsync(1).get();
    }

    @Test
    public void containsKey_quorum() {
        cache(0).containsKey(1);
    }

    @Test(expected = QuorumException.class)
    public void containsKey_noQuorum() {
        cache(3).containsKey(1);
    }

    @Test
    public void getAll_quorum() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        cache(0).getAll(hashSet);
    }

    @Test(expected = QuorumException.class)
    public void getAll_noQuorum() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        cache(3).getAll(hashSet);
    }

    @Test
    public void iterator_quorum() {
        cache(0).iterator();
    }

    @Test(expected = QuorumException.class)
    public void iterator_noQuorum() {
        cache(3).iterator();
    }

    @Test
    public void size_quorum() {
        cache(0).size();
    }

    @Test(expected = QuorumException.class)
    public void size_noQuorum() {
        cache(3).size();
    }

    @Test
    public void isDestroyed_quorum() {
        cache(0).isDestroyed();
    }

    @Test
    public void isDestroyed_noQuorum() {
        cache(3).isDestroyed();
    }

    @Test
    public void isClosed_quorum() {
        cache(0).isClosed();
    }

    @Test
    public void isClosed_noQuorum() {
        cache(3).isClosed();
    }

    @Test
    public void getLocalCacheStatistics_quorum() {
        try {
            cache(0).getLocalCacheStatistics();
        } catch (UnsupportedOperationException ex) {
            // client workaround
        }
    }

    @Test
    public void getLocalCacheStatistics_noQuorum() {
        try {
            cache(3).getLocalCacheStatistics();
        } catch (UnsupportedOperationException ex) {
            // client workaround
        }
    }

    protected ICache<Integer, String> cache(int index) {
        return cache(index, quorumType);
    }
}
