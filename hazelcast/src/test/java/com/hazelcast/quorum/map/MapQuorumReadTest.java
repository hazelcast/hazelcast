/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.quorum.map;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.config.Config;
import com.hazelcast.core.IMap;
import com.hazelcast.projection.Projections;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class})
public class MapQuorumReadTest extends AbstractMapQuorumTest {

    @Parameterized.Parameter
    public static QuorumType quorumType;

    @Parameterized.Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{QuorumType.READ}, {QuorumType.READ_WRITE}});
    }

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(new Config(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void get_successful_whenQuorumSize_met() {
        map(0).get("foo");
    }

    @Test(expected = QuorumException.class)
    public void get_failing_whenQuorumSize_notMet() {
        map(3).get("foo");
    }

    @Test
    public void getAsync_successful_whenQuorumSize_met() throws Exception {
        Future<Object> future = map(0).getAsync("foo");
        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void getAsync_failing_whenQuorumSize_notMet() throws Exception {
        Future<Object> future = map(3).getAsync("foo");
        future.get();
    }

    @Test
    public void getAll_successful_whenQuorumSize_met() {
        HashSet<Object> keys = new HashSet<Object>();
        keys.add("foo");
        map(0).getAll(keys);
    }

    @Test(expected = QuorumException.class)
    public void getAll_failing_whenQuorumSize_notMet() {
        HashSet<Object> keys = new HashSet<Object>();
        keys.add("foo");
        map(3).getAll(keys);
    }

    @Test
    public void getEntryView_successful_whenQuorumSize_met() {
        map(0).getEntryView("foo");
    }

    @Test(expected = QuorumException.class)
    public void getEntryView_failing_whenQuorumSize_notMet() {
        map(3).getEntryView("foo");
    }


    @Test
    public void containsKey_successful_whenQuorumSize_met() {
        map(0).containsKey("foo");
    }

    @Test(expected = QuorumException.class)
    public void containsKey_failing_whenQuorumSize_notMet() {
        map(3).containsKey("foo");
    }

    @Test
    public void containsValue_successful_whenQuorumSize_met() {
        map(0).containsValue("foo");
    }

    @Test(expected = QuorumException.class)
    public void containsValue_failing_whenQuorumSize_notMet() {
        map(3).containsValue("foo");
    }

    @Test
    public void keySet_successful_whenQuorumSize_met() {
        map(0).keySet();
    }

    @Test(expected = QuorumException.class)
    public void keySet_failing_whenQuorumSize_notMet() {
        map(3).keySet();
    }

    @Test
    public void localKeySet_successful_whenQuorumSize_met() {
        try {
            map(0).localKeySet();
        } catch (UnsupportedOperationException ex) {
        }
    }

    @Test(expected = QuorumException.class)
    public void localKeySet_failing_whenQuorumSize_notMet() {
        try {
            map(3).localKeySet();
        } catch (UnsupportedOperationException ex) {
            throw new QuorumException("Workaround for unsupported operation");
        }
    }

    @Test
    public void values_successful_whenQuorumSize_met() {
        map(0).values();
    }

    @Test(expected = QuorumException.class)
    public void values_failing_whenQuorumSize_notMet() {
        map(3).values();
    }

    @Test
    public void entrySet_successful_whenQuorumSize_met() {
        map(0).entrySet();
    }

    @Test(expected = QuorumException.class)
    public void entrySet_failing_whenQuorumSize_notMet() {
        map(3).entrySet();
    }

    @Test
    public void size_successful_whenQuorumSize_met() {
        map(0).size();
    }

    @Test(expected = QuorumException.class)
    public void size_failing_whenQuorumSize_notMet() {
        map(3).size();
    }

    @Test
    public void isEmpty_successful_whenQuorumSize_met() {
        map(0).isEmpty();
    }

    @Test(expected = QuorumException.class)
    public void isEmpty_failing_whenQuorumSize_notMet() {
        map(3).isEmpty();
    }

    @Test
    public void isLocked_successful_whenQuorumSize_met() {
        map(0).isLocked("foo");
    }

    @Test(expected = QuorumException.class)
    public void isLocked_failing_whenQuorumSize_notMet() {
        map(3).isLocked("foo");
    }

    @Test
    public void project_successful_whenQuorumSize_met() {
        map(0).project(Projections.singleAttribute("__key"));
    }

    @Test(expected = QuorumException.class)
    public void project_failing_whenQuorumSize_notMet() {
        map(3).project(Projections.singleAttribute("__key"));
    }

    @Test
    public void aggregate_successful_whenQuorumSize_met() {
        map(0).aggregate(Aggregators.distinct());
    }

    @Test(expected = QuorumException.class)
    public void aggregate_failing_whenQuorumSize_notMet() {
        map(3).aggregate(Aggregators.distinct());
    }

    protected IMap map(int index) {
        return map(index, quorumType);
    }
}
