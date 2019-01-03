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

package com.hazelcast.quorum.multimap;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.MultiMap;
import com.hazelcast.quorum.AbstractQuorumTest;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class MultiMapQuorumWriteTest extends AbstractQuorumTest {

    @Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{WRITE}, {READ_WRITE}});
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
    public void put_failing_whenQuorumSize_notMet() {
        map(3).put("foo", "bar");
    }

    @Test
    public void lock_successful_whenQuorumSize_met() {
        map(0).lock(UuidUtil.newUnsecureUuidString());
    }

    @Test(expected = QuorumException.class)
    public void lock_failing_whenQuorumSize_notMet() {
        map(3).lock(UuidUtil.newUnsecureUuidString());
    }

    @Test
    public void lockWithTime_successful_whenQuorumSize_met() {
        map(0).lock(UuidUtil.newUnsecureUuidString(), 5, TimeUnit.SECONDS);
    }

    @Test(expected = QuorumException.class)
    public void lockWithTime_failing_whenQuorumSize_notMet() {
        map(3).lock(UuidUtil.newUnsecureUuidString(), 5, TimeUnit.SECONDS);
    }

    @Test
    public void tryLock_successful_whenQuorumSize_met() {
        map(0).tryLock(UuidUtil.newUnsecureUuidString());
    }

    @Test(expected = QuorumException.class)
    public void tryLock_failing_whenQuorumSize_notMet() {
        map(3).tryLock(UuidUtil.newUnsecureUuidString());
    }

    @Test
    public void tryLockWithTime_successful_whenQuorumSize_met() throws InterruptedException {
        map(0).tryLock(UuidUtil.newUnsecureUuidString(), 5, TimeUnit.SECONDS);
    }

    @Test(expected = QuorumException.class)
    public void tryLockWithTime_failing_whenQuorumSize_notMet() throws InterruptedException {
        map(3).tryLock(UuidUtil.newUnsecureUuidString(), 5, TimeUnit.SECONDS);
    }

    @Test
    public void tryLockWithLease_successful_whenQuorumSize_met() throws InterruptedException {
        map(0).tryLock(UuidUtil.newUnsecureUuidString(), 5, TimeUnit.SECONDS, 5, TimeUnit.SECONDS);
    }

    @Test(expected = QuorumException.class)
    public void tryLockWithLease_failing_whenQuorumSize_notMet() throws InterruptedException {
        map(3).tryLock(UuidUtil.newUnsecureUuidString(), 5, TimeUnit.SECONDS, 5, TimeUnit.SECONDS);
    }

    @Test
    public void unlock_successful_whenQuorumSize_met() {
        try {
            map(0).unlock("foo");
        } catch (IllegalMonitorStateException ex) {
            // expected & meaningless
        }
    }

    @Test(expected = QuorumException.class)
    public void unlock_failing_whenQuorumSize_notMet() {
        try {
            map(3).unlock("foo");
        } catch (IllegalMonitorStateException ex) {
            // expected & meaningless
        }
    }

    @Test
    public void forceUnlock_successful_whenQuorumSize_met() {
        map(0).forceUnlock("foo");
    }

    @Test(expected = QuorumException.class)
    public void forceUnlock_failing_whenQuorumSize_notMet() {
        map(3).forceUnlock("foo");
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
    public void removeWhenExists_successful_whenQuorumSize_met() {
        map(0).remove("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void removeWhenExists_failing_whenQuorumSize_notMet() {
        map(3).remove("foo", "bar");
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
    public void addLocalEntryListener_successful_whenQuorumSize_met() {
        try {
            map(0).addLocalEntryListener(new EntryAdapter());
        } catch (UnsupportedOperationException ignored) {
        }
    }

    @Test
    public void addLocalEntryListener_successful_whenQuorumSize_notMet() {
        try {
            map(3).addLocalEntryListener(new EntryAdapter());
        } catch (UnsupportedOperationException ignored) {
        }
    }

    @Test
    public void addEntryListener_successful_whenQuorumSize_met() {
        map(0).addEntryListener(new EntryAdapter(), true);
    }

    @Test
    public void addEntryListener_successful_whenQuorumSize_notMet() {
        map(3).addEntryListener(new EntryAdapter(), true);
    }

    @Test
    public void addEntryListenerWithKey_successful_whenQuorumSize_met() {
        map(0).addEntryListener(new EntryAdapter(), "foo", true);
    }

    @Test
    public void addEntryListenerWithKey_successful_whenQuorumSize_notMet() {
        map(3).addEntryListener(new EntryAdapter(), "foo", true);
    }

    @Test
    public void removeEntryListenerWithKey_successful_whenQuorumSize_met() {
        map(0).removeEntryListener("id123");
    }

    @Test
    public void removeEntryListenerWithKey_successful_whenQuorumSize_notMet() {
        map(3).removeEntryListener("id123");
    }

    protected MultiMap map(int index) {
        return multimap(index, quorumType);
    }
}
