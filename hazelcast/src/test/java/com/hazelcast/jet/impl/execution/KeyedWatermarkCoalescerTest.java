/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KeyedWatermarkCoalescerTest extends JetTestSupport {

    private final KeyedWatermarkCoalescer kwc = new KeyedWatermarkCoalescer(2);

    @Test
    public void when_Q1ProducesWmPlusEventAndQ2IsIdle_then_forwardWmAndEventFromQ1() {
        kwc.observeWm(0, wm(0));
        assertEquals(singletonList(wm(0)), kwc.observeWm(1, IDLE_MESSAGE));

        kwc.observeEvent(0);
        assertEquals(singletonList(wm(1)), kwc.observeWm(0, wm(1)));
    }

    @Test
    public void when_noWmKeyKnown_and_idleMessageReceived_then_idleForwardedWhenAllIdle() {
        assertEquals(emptyList(), kwc.observeWm(0, IDLE_MESSAGE));
        assertEquals(singletonList(IDLE_MESSAGE), kwc.observeWm(1, IDLE_MESSAGE));

        assertEquals(emptySet(), kwc.keys());
    }

    @Test
    public void test_initialScenario2() {
        // no wm key known yet, receive idle message on queue 0 - 1 of 2 queues idle, nothing forwarded
        assertEquals(emptyList(), kwc.observeWm(0, IDLE_MESSAGE));

        // receive a wm on queue 0, queue 0 active again. Nothing forwarded, wm missing on queue 1
        assertEquals(emptyList(), kwc.observeWm(0, wm(10, (byte) 42)));

        // receive an idle message on queue 1
        // since queue 0 is active and queue 1 idle, the previous wm from queue 0 is forwarded
        assertEquals(singletonList(wm(10, (byte) 42)), kwc.observeWm(1, IDLE_MESSAGE));
        assertEquals(singleton((byte) 42), kwc.keys());
    }

    @Test
    public void test_initialScenario3_idleStatusTransferredToNewWmKey() {
        // idle message on queue 0, no WM key known yet
        assertEquals(emptyList(), kwc.observeWm(0, IDLE_MESSAGE));
        assertTrue(kwc.keys().isEmpty());

        // WM on queue 1, new WM key. Since queue 0 is idle, it should be forwarded immediately
        assertEquals(singletonList(wm(10, (byte) 42)), kwc.observeWm(1, wm(10, (byte) 42)));

        assertEquals(singleton((byte) 42), kwc.keys());
    }

    @Test
    public void test_initialScenario4() {
        kwc.observeWm(0, wm(42));
        assertEquals(emptyList(), kwc.observeWm(0, IDLE_MESSAGE));
        assertEquals(asList(wm(42), IDLE_MESSAGE), kwc.observeWm(1, IDLE_MESSAGE));
    }

    @Test
    public void test_oneQueueDoneOtherIdle_then_allIdle() {
        assertEquals(emptyList(), kwc.queueDone(0));
        assertEquals(singletonList(IDLE_MESSAGE), kwc.observeWm(1, IDLE_MESSAGE));
    }

    @Test
    public void test_q1IdleThenActiveThenQ2Idle_then_notAllIdle() {
        assertEquals(emptyList(), kwc.observeWm(0, IDLE_MESSAGE));
        kwc.observeEvent(0);
        assertEquals(emptyList(), kwc.observeWm(1, IDLE_MESSAGE));
    }
}
