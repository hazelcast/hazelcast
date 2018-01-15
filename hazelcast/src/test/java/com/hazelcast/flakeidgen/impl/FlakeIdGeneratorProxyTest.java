/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.config.Config;
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

import java.util.Date;

import static com.hazelcast.flakeidgen.impl.FlakeIdGeneratorProxy.BITS_NODE_ID;
import static com.hazelcast.flakeidgen.impl.FlakeIdGeneratorProxy.BITS_SEQUENCE;
import static com.hazelcast.flakeidgen.impl.FlakeIdGeneratorProxy.BITS_TIMESTAMP;
import static com.hazelcast.flakeidgen.impl.FlakeIdGeneratorProxy.EPOCH_START;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class FlakeIdGeneratorProxyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ILogger logger;
    private FlakeIdGeneratorProxy gen;

    @Before
    public void before() {
        logger = mock(ILogger.class);
        NodeEngine nodeEngine = mock(NodeEngine.class, new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                if (invocation.getMethod().getName().equals("getLogger")) {
                    return logger;
                }
                return null;
            }
        });
        when(nodeEngine.getConfig()).thenReturn(new Config());
        gen = new FlakeIdGeneratorProxy("foo", nodeEngine, null);
    }

    @Test
    public void test_usedBits() {
        assertEquals(63, BITS_NODE_ID + BITS_TIMESTAMP + BITS_SEQUENCE);
    }

    @Test
    public void test_timeMiddle() {
        assertEquals(5300086112257234L, gen.newIdBaseLocal(1516028439000L, 1234, 10));
    }

    @Test
    public void test_timeLowEdge() {
        assertEquals(1234L, gen.newIdBaseLocal(EPOCH_START, 1234, 10));
    }

    @Test
    public void test_timeHighEdge() {
        assertEquals(9223372036850582738L, gen.newIdBaseLocal(EPOCH_START + (1L << BITS_TIMESTAMP) - 1L, 1234, 10));
    }

    @Test
    public void test_idsOrdered() {
        long lastId = -1;
        for (long now = EPOCH_START;
                now < EPOCH_START + (1L << BITS_TIMESTAMP);
                now += 365L * 24L * 60L * 60L * 1000L) {
            long base = gen.newIdBaseLocal(now, 1234, 1);
            System.out.println("at " + new Date(now) + ", id=" + base);
            assertTrue("lastId=" + lastId + ", newId=" + base, lastId < base);
            lastId = base;
        }
    }

    @Test
    public void when_currentTimeBeforeAllowedRange_then_fail() {
        gen.newIdBaseLocal(EPOCH_START, 0, 1);
        exception.expect(AssertionError.class);
        exception.expectMessage("Current time out of allowed range");
        gen.newIdBaseLocal(EPOCH_START - 1, 0, 1);
    }

    @Test
    public void when_currentTimeAfterAllowedRange_then_fail() {
        gen.newIdBaseLocal(EPOCH_START + (1L << BITS_TIMESTAMP) - 1, 0, 1);
        exception.expect(AssertionError.class);
        exception.expectMessage("Current time out of allowed range");
        gen.newIdBaseLocal(EPOCH_START + (1L << BITS_TIMESTAMP), 0, 1);
    }

    @Test
    public void when_twoIdsAtTheSameMoment_then_higherSeq() {
        long id1 = gen.newIdBaseLocal(1516028439000L, 1234, 1);
        long id2 = gen.newIdBaseLocal(1516028439000L, 1234, 1);
        assertEquals(5300086112257234L, id1);
        assertEquals(id1 + (1 << BITS_NODE_ID), id2);
    }

}
