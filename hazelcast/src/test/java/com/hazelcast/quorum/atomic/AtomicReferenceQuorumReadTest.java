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
import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.isA;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class AtomicReferenceQuorumReadTest extends AbstractQuorumTest {

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
        aref(0).get();
    }

    @Test(expected = QuorumException.class)
    public void get_noQuorum() {
        aref(3).get();
    }

    @Test
    public void getAsync_quorum() throws Exception {
        aref(0).getAsync().get();
    }

    @Test
    public void getAsync_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        aref(3).getAsync().get();
    }

    @Test
    public void isNull_quorum() {
        aref(0).isNull();
    }

    @Test(expected = QuorumException.class)
    public void isNull_noQuorum() {
        aref(3).isNull();
    }

    @Test
    public void isNullAsync_quorum() throws Exception {
        aref(0).isNullAsync().get();
    }

    @Test
    public void isNullAsync_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        aref(3).isNullAsync().get();
    }

    @Test
    public void contains_quorum() {
        aref(0).contains(object());
    }

    @Test(expected = QuorumException.class)
    public void contains_noQuorum() {
        aref(3).contains(object());
    }

    @Test
    public void containsAsync_quorum() throws Exception {
        aref(0).containsAsync(object()).get();
    }

    @Test
    public void containsAsync_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        aref(3).containsAsync(object()).get();
    }

    private IAtomicReference<QuorumTestClass> aref(int index) {
        return aref(index, quorumType);
    }
}
