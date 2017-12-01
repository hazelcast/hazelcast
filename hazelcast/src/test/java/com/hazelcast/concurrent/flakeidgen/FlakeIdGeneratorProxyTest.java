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

package com.hazelcast.concurrent.flakeidgen;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static com.hazelcast.concurrent.flakeidgen.FlakeIdGeneratorProxy.BITS_TIMESTAMP;
import static com.hazelcast.concurrent.flakeidgen.FlakeIdGeneratorProxy.EPOCH_START;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class FlakeIdGeneratorProxyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private NodeEngine nodeEngine;
    private ILogger logger;

    @Before
    public void before() {
        logger = mock(ILogger.class);
        nodeEngine = mock(NodeEngine.class, new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                if (invocation.getMethod().getName().equals("getLogger")) {
                    return logger;
                }
                return null;
            }
        });
    }

    @Test
    public void test_timeInNegativeRange() {
        FlakeIdGeneratorProxy gen = createGenerator(1234);
        assertEquals(-9112343572002110254L, gen.newIdBaseLocal(1509700048830L, 10));
    }

    @Test
    public void test_timeInPositiveRange() {
        FlakeIdGeneratorProxy gen = createGenerator(1234);
        assertEquals(41798522940425426L, gen.newIdBaseLocal(3692217600000L, 10));
    }

    @Test
    public void test_idsOrdered() {
        FlakeIdGeneratorProxy gen = createGenerator(1234);
        long lastId = Long.MIN_VALUE;
        for (
                long now = EPOCH_START - (1L << BITS_TIMESTAMP - 1);
                now < EPOCH_START + (1L << BITS_TIMESTAMP - 1);
                now += 365L * 24L * 60L * 60L * 1000L) {
            long base = gen.newIdBaseLocal(now, 1);
//            System.out.println("at " + new Date(now) + ", id=" + base);
            assertTrue("lastId=" + lastId + ", newId=" + base, lastId < base);
            lastId = base;
        }
    }

    @Test
    public void when_nodeIdTooLarge_then_fail() {
        FlakeIdGeneratorProxy gen = createGenerator(65536);
        exception.expect(FlakeIdNodeIdOutOfRangeException.class);
        gen.newId();
    }

    @Test
    public void when_nodeIdTooSmall_then_fail() {
        FlakeIdGeneratorProxy gen = createGenerator(-1);
        exception.expect(FlakeIdNodeIdOutOfRangeException.class);
        gen.newId();
    }

    @Test
    public void when_currentTimeBeforeAllowedRange_then_fail() {
        FlakeIdGeneratorProxy gen = createGenerator(0);
        gen.newIdBaseLocal(EPOCH_START - (1L << BITS_TIMESTAMP - 1), 1);
        exception.expect(AssertionError.class);
        exception.expectMessage("Current time out of allowed range");
        gen.newIdBaseLocal(EPOCH_START - (1L << BITS_TIMESTAMP - 1) - 1, 1);
    }

    @Test
    public void when_currentTimeAfterAllowedRange_then_fail() {
        FlakeIdGeneratorProxy gen = createGenerator(0);
        gen.newIdBaseLocal(EPOCH_START + (1L << BITS_TIMESTAMP - 1) - 1, 1);
        exception.expect(AssertionError.class);
        exception.expectMessage("Current time out of allowed range");
        gen.newIdBaseLocal(EPOCH_START + (1L << BITS_TIMESTAMP - 1), 1);
    }

    @Test
    public void when_twoIdsAtTheSameMoment_then_higherSeq() {
        FlakeIdGeneratorProxy gen = createGenerator(1234);
        long id1 = gen.newIdBaseLocal(1509700048830L, 1);
        long id2 = gen.newIdBaseLocal(1509700048830L, 1);
        long[] ids = new long[]{id1, id2};
        assertEquals(-9112343572002110254L, ids[0]);
        long increment = gen.newIdBatch(1).increment();
        assertEquals(ids[0] + increment, ids[1]);
    }

    private FlakeIdGeneratorProxy createGenerator(int nodeId) {
        return new FlakeIdGeneratorProxy("foo", nodeId, nodeEngine, null);
    }
}
