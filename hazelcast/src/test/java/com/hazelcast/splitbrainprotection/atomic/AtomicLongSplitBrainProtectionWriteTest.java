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

package com.hazelcast.splitbrainprotection.atomic;

import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionTest;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
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
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.READ_WRITE;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.WRITE;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.isA;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AtomicLongSplitBrainProtectionWriteTest extends AbstractSplitBrainProtectionTest {

    @Parameters(name = "splitBrainProtectionType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{WRITE}, {READ_WRITE}});
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
    public void addAndGet() {
        along(0).addAndGet(1);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void addAndGet_noSplitBrainProtection() {
        along(3).addAndGet(1);
    }

    @Test
    public void addAndGetAsync() throws Exception {
        along(0).addAndGetAsync(1).get();
    }

    @Test
    public void addAndGetAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        along(3).addAndGetAsync(1).get();
    }

    @Test
    public void alter() {
        along(0).alter(function());
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void alter_noSplitBrainProtection() {
        along(3).alter(function());
    }

    @Test
    public void alterAsync() throws Exception {
        along(0).alterAsync(function()).get();
    }

    @Test
    public void alterAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        along(3).alterAsync(function()).get();
    }

    @Test
    public void alterAndGet() {
        along(0).alterAndGet(function());
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void alterAndGet_noSplitBrainProtection() {
        along(3).alterAndGet(function());
    }

    @Test
    public void alterAndGetAsync() throws Exception {
        along(0).alterAndGetAsync(function()).get();
    }

    @Test
    public void alterAndGetAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        along(3).alterAndGetAsync(function()).get();
    }

    @Test
    public void apply() {
        along(0).apply(function());
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void apply_noSplitBrainProtection() {
        along(3).apply(function());
    }

    @Test
    public void applyAsync() throws Exception {
        along(0).applyAsync(function()).get();
    }

    @Test
    public void applyAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        along(3).applyAsync(function()).get();
    }

    @Test
    public void compareAndSet() {
        along(0).compareAndSet(1L, 2L);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void compareAndSet_noSplitBrainProtection() {
        along(3).compareAndSet(1L, 2L);
    }

    @Test
    public void compareAndSetAsync() throws Exception {
        along(0).compareAndSetAsync(1L, 2L).get();
    }

    @Test
    public void compareAndSetAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        along(3).compareAndSetAsync(1L, 2L).get();
    }

    @Test
    public void decrementAndGet() {
        along(0).decrementAndGet();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void decrementAndGet_noSplitBrainProtection() {
        along(3).decrementAndGet();
    }

    @Test
    public void decrementAndGetAsync() throws Exception {
        along(0).decrementAndGetAsync().get();
    }

    @Test
    public void decrementAndGetAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        along(3).decrementAndGetAsync().get();
    }

    @Test
    public void getAndAdd() {
        along(0).getAndAdd(1L);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAndAdd_noSplitBrainProtection() {
        along(3).getAndAdd(1L);
    }

    @Test
    public void getAndAddAsync() throws Exception {
        along(0).getAndAddAsync(1L).get();
    }

    @Test
    public void getAndAddAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        along(3).getAndAddAsync(1L).get();
    }

    @Test
    public void getAndAlter() {
        along(0).getAndAlter(function());
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAndAlter_noSplitBrainProtection() {
        along(3).getAndAlter(function());
    }

    @Test
    public void getAndAlterAsync() throws Exception {
        along(0).getAndAlterAsync(function()).get();
    }

    @Test
    public void getAndAlterAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        along(3).getAndAlterAsync(function()).get();
    }

    @Test
    public void getAndIncrement() {
        along(0).getAndIncrement();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAndIncrement_noSplitBrainProtection() {
        along(3).getAndIncrement();
    }

    @Test
    public void getAndIncrementAsync() throws Exception {
        along(0).getAndIncrementAsync().get();
    }

    @Test
    public void getAndIncrementAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        along(3).getAndIncrementAsync().get();
    }

    @Test
    public void getAndSet() {
        along(0).getAndSet(2L);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAndSet_noSplitBrainProtection() {
        along(3).getAndSet(2L);
    }

    @Test
    public void getAndSetAsync() throws Exception {
        along(0).getAndSetAsync(2L).get();
    }

    @Test
    public void getAndSetAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        along(3).getAndSetAsync(2L).get();
    }

    @Test
    public void incrementAndGet() {
        along(0).incrementAndGet();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void incrementAndGet_noSplitBrainProtection() {
        along(3).incrementAndGet();
    }

    @Test
    public void incrementAndGetAsync() throws Exception {
        along(0).incrementAndGetAsync().get();
    }

    @Test
    public void incrementAndGetAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        along(3).incrementAndGetAsync().get();
    }

    @Test
    public void set() {
        along(0).set(2L);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void set_noSplitBrainProtection() {
        along(3).set(2L);
    }

    @Test
    public void setAsync() throws Exception {
        along(0).setAsync(2L).get();
    }

    @Test
    public void setAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        along(3).setAsync(2L).get();
    }

    private IAtomicLong along(int index) {
        return along(index, splitBrainProtectionOn);
    }
}
