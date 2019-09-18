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

import com.hazelcast.cp.IAtomicReference;
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

import static com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionTest.SplitBrainProtectionTestClass.object;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.READ_WRITE;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.WRITE;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.isA;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AtomicReferenceSplitBrainProtectionWriteTest extends AbstractSplitBrainProtectionTest {

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
    public void getAndAlter_splitBrainProtection() {
        aref(0).getAndAlter(function());
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAndAlter_noSplitBrainProtection() {
        aref(3).getAndAlter(function());
    }

    @Test
    public void getAndAlterAsync_splitBrainProtection() throws Exception {
        aref(0).getAndAlterAsync(function()).get();
    }

    @Test
    public void getAndAlterAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        aref(3).getAndAlterAsync(function()).get();
    }

    @Test
    public void alterAndGetAsync_splitBrainProtection() throws Exception {
        aref(0).alterAndGetAsync(function()).get();
    }

    @Test
    public void alterAndGetAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        aref(3).alterAndGetAsync(function()).get();
    }

    @Test
    public void alterAndGet_splitBrainProtection() {
        aref(0).alterAndGet(function());
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void alterAndGet_noSplitBrainProtection() {
        aref(3).alterAndGet(function());
    }

    @Test
    public void alterAsync_splitBrainProtection() throws Exception {
        aref(0).alterAsync(function()).get();
    }

    @Test
    public void alterAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        aref(3).alterAsync(function()).get();
    }

    @Test
    public void alter_splitBrainProtection() {
        aref(0).alter(function());
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void alter_noSplitBrainProtection() {
        aref(3).alter(function());
    }

    @Test
    public void applyAsync_splitBrainProtection() throws Exception {
        aref(0).applyAsync(function()).get();
    }

    @Test
    public void applyAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        aref(3).applyAsync(function()).get();
    }

    @Test
    public void apply_splitBrainProtection() {
        aref(0).apply(function());
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void apply_noSplitBrainProtection() {
        aref(3).apply(function());
    }

    @Test
    public void clearAsync_splitBrainProtection() throws Exception {
        aref(0).clearAsync().get();
    }

    @Test
    public void clearAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        aref(3).clearAsync().get();
    }

    @Test
    public void clear_splitBrainProtection() {
        aref(0).clear();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void clear_noSplitBrainProtection() {
        aref(3).clear();
    }

    @Test
    public void compareAndSetAsync_splitBrainProtection() throws Exception {
        aref(0).compareAndSetAsync(object(), object()).get();
    }

    @Test
    public void compareAndSetAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        aref(3).compareAndSetAsync(object(), object()).get();
    }

    @Test
    public void compareAndSet_splitBrainProtection() {
        aref(0).compareAndSet(object(), object());
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void compareAndSet_noSplitBrainProtection() {
        aref(3).compareAndSet(object(), object());
    }

    @Test
    public void getAndSetAsync_splitBrainProtection() throws Exception {
        aref(0).getAndSetAsync(object()).get();
    }

    @Test
    public void getAndSetAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        aref(3).getAndSetAsync(object()).get();
    }

    @Test
    public void getAndSet_splitBrainProtection() {
        aref(0).getAndSet(object());
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAndSet_noSplitBrainProtection() {
        aref(3).getAndSet(object());
    }

    @Test
    public void setAsync_splitBrainProtection() throws Exception {
        aref(0).setAsync(object()).get();
    }

    @Test
    public void setAsync_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        aref(3).setAsync(object()).get();
    }

    @Test
    public void set_splitBrainProtection() {
        aref(0).set(object());
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void set_noSplitBrainProtection() {
        aref(3).set(object());
    }

    private IAtomicReference aref(int index) {
        return aref(index, splitBrainProtectionOn);
    }
}
