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

package com.hazelcast.quorum.list;

import com.hazelcast.config.Config;
import com.hazelcast.core.IList;
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

import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class})
public class ListQuorumWriteTest extends AbstractListQuorumTest {

    @Parameterized.Parameter
    public static QuorumType quorumType;

    @Parameterized.Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{WRITE}, {READ_WRITE}});
    }

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(new Config(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    // add(E e);
    // remove(Object o);
    // addAll(Collection<? extends E> c);
    // retainAll(Collection<?> c);
    // removeAll(Collection<?> c);
    // clear();
    // remove();
    // set();

    @Test
    public void addOperation_successful_whenQuorumSize_met() {
        list(0).add("foo");
    }

    @Test(expected = QuorumException.class)
    public void addOperation_successful_whenQuorumSize_notMet() {
        list(3).add("foo");
    }

    @Test
    public void addAllOperation_successful_whenQuorumSize_met() {
        list(0).addAll(asList("foo", "bar"));
    }

    @Test(expected = QuorumException.class)
    public void addAllOperation_successful_whenQuorumSize_notMet() {
        list(3).add(asList("foo", "bar"));
    }

    @Test
    public void removeOperation_successful_whenQuorumSize_met() {
        list(0).remove("foo");
    }

    @Test(expected = QuorumException.class)
    public void removeOperation_successful_whenQuorumSize_notMet() {
        list(3).remove("foo");
    }

    @Test
    public void compareAndRemoveOperation_removeAll_successful_whenQuorumSize_met() {
        list(0).removeAll(asList("foo", "bar"));
    }

    @Test(expected = QuorumException.class)
    public void compareAndRemoveOperation_removeAll_successful_whenQuorumSize_notMet() {
        list(3).removeAll(asList("foo", "bar"));
    }

    @Test
    public void compareAndRemoveOperation_retainAll_successful_whenQuorumSize_met() {
        list(0).removeAll(asList("foo", "bar"));
    }

    @Test(expected = QuorumException.class)
    public void compareAndRemoveOperation_retainAll_successful_whenQuorumSize_notMet() {
        list(3).removeAll(asList("foo", "bar"));
    }

    @Test
    public void clearOperation_successful_whenQuorumSize_met() {
        list(0).clear();
        list(0).add("object123");
    }

    @Test(expected = QuorumException.class)
    public void clearOperation_successful_whenQuorumSize_notMet() {
        list(3).clear();
    }

    @Test
    public void setOperation_successful_whenQuorumSize_met() {
        try {
            list(0).set(0, "bar");
        } catch(IndexOutOfBoundsException ex) {
            // meaningless
        }
    }

    @Test(expected = QuorumException.class)
    public void setOperation_successful_whenQuorumSize_notMet() {
        list(3).set(0, "bar");
    }

    protected IList list(int index) {
        return list(index, quorumType);
    }

}
