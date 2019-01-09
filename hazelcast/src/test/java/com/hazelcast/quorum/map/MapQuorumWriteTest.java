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

package com.hazelcast.quorum.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.IMap;
import com.hazelcast.map.TestLoggingEntryProcessor;
import com.hazelcast.quorum.AbstractQuorumTest;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
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
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.InterceptorTest.SimpleInterceptor;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapQuorumWriteTest extends AbstractQuorumTest {

    @Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{QuorumType.WRITE}, {QuorumType.READ_WRITE}});
    }

    @Parameter
    public static QuorumType quorumType;

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(new Config(), new TestHazelcastInstanceFactory());
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
    public void put_failing_whenQuorumSize_met() {
        map(3).put("foo", "bar");
    }

    @Test
    public void tryPut_successful_whenQuorumSize_met() {
        map(0).tryPut("foo", "bar", 5, TimeUnit.SECONDS);
    }

    @Test(expected = QuorumException.class)
    public void tryPut_failing_whenQuorumSize_met() {
        map(3).tryPut("foo", "bar", 5, TimeUnit.SECONDS);
    }

    @Test
    public void putTransient_successful_whenQuorumSize_met() {
        map(0).putTransient("foo", "bar", 5, TimeUnit.SECONDS);
    }

    @Test(expected = QuorumException.class)
    public void putTransient_failing_whenQuorumSize_met() {
        map(3).putTransient("foo", "bar", 5, TimeUnit.SECONDS);
    }

    @Test
    public void putIfAbsent_successful_whenQuorumSize_met() {
        map(0).putIfAbsent("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void putIfAbsent_failing_whenQuorumSize_met() {
        map(3).putIfAbsent("foo", "bar");
    }

    @Test
    public void putAsync_successful_whenQuorumSize_met() throws Exception {
        map(0).putAsync("foo", "bar").get();
    }

    @Test(expected = ExecutionException.class)
    public void putAsync_failing_whenQuorumSize_met() throws Exception {
        map(3).putAsync("foo", "bar").get();
    }

    @Test
    public void putAll_successful_whenQuorumSize_met() {
        HashMap<Object, Object> map = new HashMap<Object, Object>();
        map.put("foo", "bar");
        map(0).putAll(map);
    }

    @Test(expected = QuorumException.class)
    public void putAll_failing_whenQuorumSize_met() {
        HashMap<Object, Object> map = new HashMap<Object, Object>();
        map.put("foo", "bar");
        map(3).putAll(map);
    }

    @Test
    public void remove_successful_whenQuorumSize_met() {
        map(0).remove("foo");
    }

    @Test(expected = QuorumException.class)
    public void remove_failing_whenQuorumSize_met() {
        map(3).remove("foo");
    }

    @Test
    public void removeWhenExists_successful_whenQuorumSize_met() {
        map(0).remove("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void removeWhenExists_failing_whenQuorumSize_met() {
        map(3).remove("foo", "bar");
    }

    @Test
    public void removeAsync_successful_whenQuorumSize_met() throws Exception {
        map(0).removeAsync("foo").get();
    }

    @Test(expected = ExecutionException.class)
    public void removeAsync_failing_whenQuorumSize_met() throws Exception {
        map(3).removeAsync("foo").get();
    }

    @Test
    public void delete_successful_whenQuorumSize_met() {
        map(0).delete("foo");
    }

    @Test(expected = QuorumException.class)
    public void delete_failing_whenQuorumSize_met() {
        map(3).delete("foo");
    }

    @Test
    public void clear_successful_whenQuorumSize_met() {
        map(0).clear();
    }

    @Test(expected = QuorumException.class)
    public void clear_failing_whenQuorumSize_met() {
        map(3).clear();
    }

    @Test
    public void set_successful_whenQuorumSize_met() {
        map(0).set("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void set_failing_whenQuorumSize_met() {
        map(3).set("foo", "bar");
    }

    @Test
    public void replace_successful_whenQuorumSize_met() {
        map(0).replace("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void replace_failing_whenQuorumSize_met() {
        map(3).replace("foo", "bar");
    }

    @Test
    public void replaceIfExists_successful_whenQuorumSize_met() {
        map(0).replace("foo", "bar", "baz");
    }

    @Test(expected = QuorumException.class)
    public void replaceIfExists_failing_whenQuorumSize_met() {
        map(3).replace("foo", "bar", "baz");
    }

    @Test
    public void tryRemove_successful_whenQuorumSize_met() {
        map(0).tryRemove("foo", 5, TimeUnit.SECONDS);
    }

    @Test(expected = QuorumException.class)
    public void tryRemove_failing_whenQuorumSize_met() {
        map(3).tryRemove("foo", 5, TimeUnit.SECONDS);
    }

    @Test
    public void flush_successful_whenQuorumSize_met() {
        map(0).flush();
    }

    @Test(expected = QuorumException.class)
    public void flush_failing_whenQuorumSize_met() {
        map(3).flush();
    }

    @Test
    public void evictAll_successful_whenQuorumSize_met() {
        map(0).evictAll();
    }

    @Test(expected = QuorumException.class)
    public void evictAll_failing_whenQuorumSize_met() {
        map(3).evictAll();
    }

    @Test
    public void evictWhenExists_successful_whenQuorumSize_met() {
        map(0).evict("foo");
    }

    @Test(expected = QuorumException.class)
    public void evictWhenExists_failing_whenQuorumSize_met() {
        map(3).evict("foo");
    }

    @Test
    public void addIndex_successful_whenQuorumSize_met() {
        map(0).addIndex("__key", false);
    }

    @Test(expected = QuorumException.class)
    public void addIndex_failing_whenQuorumSize_met() {
        map(3).addIndex("__key", false);
    }

    @Test
    public void addInterceptor_successful_whenQuorumSize_met() {
        map(0).addInterceptor(new SimpleInterceptor());
    }

    @Test(expected = QuorumException.class)
    public void addInterceptor_failing_whenQuorumSize_met() {
        map(3).addInterceptor(new SimpleInterceptor());
    }

    @Test
    public void removeInterceptor_successful_whenQuorumSize_met() {
        map(0).removeInterceptor("foo");
    }

    @Test(expected = QuorumException.class)
    public void removeInterceptor_failing_whenQuorumSize_met() {
        map(3).removeInterceptor("foo");
    }

    @Test
    public void executeOnKey_successful_whenQuorumSize_met() {
        map(0).executeOnKey("foo", new TestLoggingEntryProcessor());
    }

    @Test(expected = QuorumException.class)
    public void executeOnKey_failing_whenQuorumSize_met() {
        map(3).executeOnKey("foo", new TestLoggingEntryProcessor());
    }

    @Test
    public void executeOnKeys_successful_whenQuorumSize_met() {
        HashSet<Object> keys = new HashSet<Object>();
        keys.add("foo");
        map(0).executeOnKey(keys, new TestLoggingEntryProcessor());
    }

    @Test(expected = QuorumException.class)
    public void executeOnKeys_failing_whenQuorumSize_met() {
        HashSet<Object> keys = new HashSet<Object>();
        keys.add("foo");
        map(3).executeOnKey(keys, new TestLoggingEntryProcessor());
    }

    @Test
    public void executeOnEntries_successful_whenQuorumSize_met() {
        map(0).executeOnEntries(new TestLoggingEntryProcessor());
    }

    @Test(expected = QuorumException.class)
    public void executeOnEntries_failing_whenQuorumSize_met() {
        map(3).executeOnEntries(new TestLoggingEntryProcessor());
    }

    @Test
    public void submitToKey_successful_whenQuorumSize_met() throws Exception {
        map(0).submitToKey("foo", new TestLoggingEntryProcessor()).get();
    }

    @Test(expected = ExecutionException.class)
    public void submitToKey_failing_whenQuorumSize_met() throws Exception {
        map(3).submitToKey("foo", new TestLoggingEntryProcessor()).get();
    }

    protected IMap map(int index) {
        return map(index, quorumType);
    }
}
