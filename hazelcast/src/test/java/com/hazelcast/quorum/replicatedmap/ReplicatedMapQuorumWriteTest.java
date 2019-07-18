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

package com.hazelcast.quorum.replicatedmap;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.quorum.AbstractQuorumTest;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.replicatedmap.ReplicatedMap;
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

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapQuorumWriteTest extends AbstractQuorumTest {

    @Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{WRITE}, {READ_WRITE}});
    }

    @Parameter
    public static QuorumType quorumType;

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(smallInstanceConfig(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void put_successful_whenQuorumSize_met() {
        map(0).put("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void put_failing_whenQuorumSize_notMet() {
        map(3).put("foo", "bar");
    }

    @Test
    public void putWithTtl_successful_whenQuorumSize_met() {
        map(0).put("foo", "bar", 10, TimeUnit.MINUTES);
    }

    @Test(expected = QuorumException.class)
    public void putWithTtl_failing_whenQuorumSize_notMet() {
        map(3).put("foo", "bar", 10, TimeUnit.MINUTES);
    }

    @Test
    public void putAll_successful_whenQuorumSize_met() {
        HashMap<Object, Object> map = new HashMap<Object, Object>();
        map.put("foo", "bar");
        map(0).putAll(map);
    }

    @Test(expected = QuorumException.class)
    public void putAll_failing_whenQuorumSize_notMet() {
        HashMap<Object, Object> map = new HashMap<Object, Object>();
        map.put("foo", "bar");
        map(3).putAll(map);
    }

    @Test
    public void remove_successful_whenQuorumSize_met() {
        map(0).remove("foo");
    }

    @Test(expected = QuorumException.class)
    public void remove_failing_whenQuorumSize_notMet() {
        map(3).remove("foo");
    }

    @Test
    public void clear_successful_whenQuorumSize_met() {
        map(0).clear();
    }

    @Test(expected = QuorumException.class)
    public void clear_failing_whenQuorumSize_notMet() {
        map(3).clear();
    }

    @Test
    public void addEntryListener_successful_whenQuorumSize_met() {
        map(0).addEntryListener(new EntryAdapter() {
        });
    }

    @Test
    public void addEntryListener_successful_whenQuorumSize_notMet() {
        map(3).addEntryListener(new EntryAdapter() {
        });
    }

    protected ReplicatedMap map(int index) {
        return replmap(index, quorumType);
    }
}
