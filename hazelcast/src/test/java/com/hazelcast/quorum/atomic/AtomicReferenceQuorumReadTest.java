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

package com.hazelcast.quorum.atomic;

import com.hazelcast.config.Config;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.quorum.atomic.AbstractAtomicQuorumTest.Objekt.object;
import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.isA;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class})
public class AtomicReferenceQuorumReadTest extends AbstractAtomicQuorumTest {

    @Parameterized.Parameter
    public static QuorumType quorumType;

    @Parameterized.Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{QuorumType.READ}, {QuorumType.READ_WRITE}});
    }

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

    protected IAtomicReference aref(int index) {
        return aref(index, quorumType);
    }
}
