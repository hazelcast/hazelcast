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

package com.hazelcast.quorum.atomic;

import com.hazelcast.config.Config;
import com.hazelcast.core.IAtomicReference;
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

import static com.hazelcast.quorum.AbstractQuorumTest.QuorumTestClass.object;
import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.isA;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class AtomicReferenceQuorumWriteTest extends AbstractQuorumTest {

    @Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{WRITE}, {READ_WRITE}});
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
    public void getAndAlter_quorum() {
        aref(0).getAndAlter(function());
    }

    @Test(expected = QuorumException.class)
    public void getAndAlter_noQuorum() {
        aref(3).getAndAlter(function());
    }

    @Test
    public void getAndAlterAsync_quorum() throws Exception {
        aref(0).getAndAlterAsync(function()).get();
    }

    @Test
    public void getAndAlterAsync_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        aref(3).getAndAlterAsync(function()).get();
    }

    @Test
    public void alterAndGetAsync_quorum() throws Exception {
        aref(0).alterAndGetAsync(function()).get();
    }

    @Test
    public void alterAndGetAsync_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        aref(3).alterAndGetAsync(function()).get();
    }

    @Test
    public void alterAndGet_quorum() {
        aref(0).alterAndGet(function());
    }

    @Test(expected = QuorumException.class)
    public void alterAndGet_noQuorum() {
        aref(3).alterAndGet(function());
    }

    @Test
    public void alterAsync_quorum() throws Exception {
        aref(0).alterAsync(function()).get();
    }

    @Test
    public void alterAsync_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        aref(3).alterAsync(function()).get();
    }

    @Test
    public void alter_quorum() {
        aref(0).alter(function());
    }

    @Test(expected = QuorumException.class)
    public void alter_noQuorum() {
        aref(3).alter(function());
    }

    @Test
    public void applyAsync_quorum() throws Exception {
        aref(0).applyAsync(function()).get();
    }

    @Test
    public void applyAsync_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        aref(3).applyAsync(function()).get();
    }

    @Test
    public void apply_quorum() {
        aref(0).apply(function());
    }

    @Test(expected = QuorumException.class)
    public void apply_noQuorum() {
        aref(3).apply(function());
    }

    @Test
    public void clearAsync_quorum() throws Exception {
        aref(0).clearAsync().get();
    }

    @Test
    public void clearAsync_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        aref(3).clearAsync().get();
    }

    @Test
    public void clear_quorum() {
        aref(0).clear();
    }

    @Test(expected = QuorumException.class)
    public void clear_noQuorum() {
        aref(3).clear();
    }

    @Test
    public void compareAndSetAsync_quorum() throws Exception {
        aref(0).compareAndSetAsync(object(), object()).get();
    }

    @Test
    public void compareAndSetAsync_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        aref(3).compareAndSetAsync(object(), object()).get();
    }

    @Test
    public void compareAndSet_quorum() {
        aref(0).compareAndSet(object(), object());
    }

    @Test(expected = QuorumException.class)
    public void compareAndSet_noQuorum() {
        aref(3).compareAndSet(object(), object());
    }

    @Test
    public void getAndSetAsync_quorum() throws Exception {
        aref(0).getAndSetAsync(object()).get();
    }

    @Test
    public void getAndSetAsync_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        aref(3).getAndSetAsync(object()).get();
    }

    @Test
    public void getAndSet_quorum() {
        aref(0).getAndSet(object());
    }

    @Test(expected = QuorumException.class)
    public void getAndSet_noQuorum() {
        aref(3).getAndSet(object());
    }

    @Test
    public void setAndGet_quorum() {
        aref(0).setAndGet(object());
    }

    @Test(expected = QuorumException.class)
    public void setAndGet_noQuorum() {
        aref(3).setAndGet(object());
    }

    @Test
    public void setAsync_quorum() throws Exception {
        aref(0).setAsync(object()).get();
    }

    @Test
    public void setAsync_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        aref(3).setAsync(object()).get();
    }

    @Test
    public void set_quorum() {
        aref(0).set(object());
    }

    @Test(expected = QuorumException.class)
    public void set_noQuorum() {
        aref(3).set(object());
    }

    private IAtomicReference aref(int index) {
        return aref(index, quorumType);
    }
}
