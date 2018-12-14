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

package com.hazelcast.jet.impl.execution;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

public class WatermarkCoalescerTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private WatermarkCoalescer wc = WatermarkCoalescer.create(20, 2);

    @Test
    public void when_nothingHappened_then_noWm() {
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(0));
    }

    @Test
    public void when_bothInputsHaveWm_then_forwarded() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 1));
        assertEquals(1, wc.topObservedWm());
        assertEquals(Long.MIN_VALUE, wc.coalescedWm());
        assertEquals(1, wc.observeWm(0, 1, 2));
        assertEquals(2, wc.observeWm(0, 0, 3));
        assertEquals(3, wc.observeWm(0, 1, 4));
        assertEquals(4, wc.topObservedWm());
        assertEquals(3, wc.coalescedWm());
    }

    @Test
    public void when_i1RecoversFromIdleByEvent_then_wmFromI1Coalesced() {
        when_i1RecoversFromIdle_then_wmFromI1Coalesced("event");
    }

    @Test
    public void when_i1RecoversFromIdleByWatermark_then_wmFromI1Coalesced() {
        when_i1RecoversFromIdle_then_wmFromI1Coalesced("wm");
    }

    private void when_i1RecoversFromIdle_then_wmFromI1Coalesced(String idleInputActivity) {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        assertEquals(11, wc.observeWm(0, 1, 11)); // forwarded immediately
        // When
        if (idleInputActivity.equals("wm")) {
            assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 11));
        } else {
            wc.observeEvent(0);
        }
        // Then
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 1, 12)); // not forwarded, waiting for i1
        assertEquals(12, wc.observeWm(0, 0, 13)); // forwarded, both are at least at 12
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(19)));
        assertEquals(13, wc.checkWmHistory(MILLISECONDS.toNanos(20))); // forwarded 13 from i1 after a delay
    }

    @Test
    public void when_i1Idle_i2HasWm_then_forwardedImmediately() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        assertEquals(100, wc.observeWm(0, 1, 100));
    }

    @Test
    public void when_i1HasWm_i2Idle_then_forwardedImmediately() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 100));
        assertEquals(100, wc.observeWm(0, 1, IDLE_MESSAGE.timestamp()));
    }

    @Test
    public void when_i1_active_i2_active_then_wmForwardedImmediately() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 100));
        assertEquals(100, wc.observeWm(0, 1, 101));
        assertEquals(101, wc.observeWm(0, 0, 101));
    }

    @Test
    public void when_i1_active_i2_activeNoWm_then_wmForwardedAfterDelay() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 100));
        wc.observeEvent(1);
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(19)));
        assertEquals(100, wc.checkWmHistory(MILLISECONDS.toNanos(20)));
    }

    @Test
    public void when_i1_activeNoWm_i2_active_then_wmForwardedAfterDelay() {
        wc.observeEvent(1);
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 100));
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(19)));
        assertEquals(100, wc.checkWmHistory(MILLISECONDS.toNanos(20)));
    }

    @Test
    public void when_i1_active_i2_idle_then_wmForwardedImmediately() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 100));
        assertEquals(100, wc.observeWm(0, 1, IDLE_MESSAGE.timestamp()));
    }

    @Test
    public void when_i1_idle_i2_active_then_wmForwardedImmediately() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        assertEquals(100, wc.observeWm(0, 1, 100));
    }

    @Test
    public void when_i1_activeNoWm_i2_activeNoWm_then_wmForwardedAfterDelay() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 100));
        wc.observeEvent(0);
        wc.observeEvent(1);
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(19)));
        assertEquals(100, wc.checkWmHistory(MILLISECONDS.toNanos(20)));
    }

    @Test
    public void when_i1_activeNoWm_i2_idle_then_noWmToForward() {
        wc.observeEvent(0);
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 1, IDLE_MESSAGE.timestamp()));
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(19)));
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(20)));
    }

    @Test
    public void when_i1_idle_i2_activeNoWm_then_wmForwardedAfterADelay() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        wc.observeEvent(1);
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(19)));
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(20)));
    }

    @Test
    public void when_i1_idle_i2_idle_then_idleMessageForwardedImmediately() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        assertEquals(IDLE_MESSAGE.timestamp(), wc.observeWm(0, 1, IDLE_MESSAGE.timestamp()));
    }

    @Test
    public void when_i1_active_i2_done_then_forwardImmediately() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 100));
        assertEquals(100, wc.queueDone(1));
    }

    @Test
    public void when_i1_done_i2_active_then_forwardImmediately() {
        assertEquals(Long.MIN_VALUE, wc.queueDone(0));
        assertEquals(100, wc.observeWm(0, 1, 100));
    }

    @Test
    public void when_i1_idle_i2_done_i1_recovers_then_idleMessageForwardedImmediately() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        assertEquals(IDLE_MESSAGE.timestamp(), wc.queueDone(1));
        assertEquals(10, wc.observeWm(0, 0, 10));
    }

    @Test
    public void when_i1_done_i2_idleAndRecovers_then_wmsForwardedImmediately() {
        assertEquals(Long.MIN_VALUE, wc.queueDone(0));
        assertEquals(IDLE_MESSAGE.timestamp(), wc.observeWm(0, 1, IDLE_MESSAGE.timestamp()));
        assertEquals(10, wc.observeWm(0, 1, 10));
    }

    @Test
    public void when_duplicateIdleMessage_then_processed() {
        // Duplicate idle messages are possible in this scenario:
        // A source instance emits IDLE_MESSAGE, then an event (not causing a WM) and then another
        // IDLE_MESSAGE again. The IDLE_MESSAGE is broadcast, but the event is not. So a downstream
        // instance can receive two IDLE_MESSAGE-s in a row.
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
    }

    @Test
    public void when_allIdleAndDuplicateIdleMessage_then_processed() {
        // Duplicate idle messages are possible in this scenario:
        // A source instance emits IDLE_MESSAGE, then an event (not causing a WM) and then another
        // IDLE_MESSAGE again. The IDLE_MESSAGE is broadcast, but the event is not. So a downstream
        // instance can receive two IDLE_MESSAGE-s in a row.
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        assertEquals(IDLE_MESSAGE.timestamp(), wc.observeWm(0, 1, IDLE_MESSAGE.timestamp()));
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
    }

    @Test
    public void when_allDone_then_noMaxValueEmitted() {
        assertEquals(Long.MIN_VALUE, wc.queueDone(0));
        assertEquals(Long.MIN_VALUE, wc.queueDone(1));
    }

    @Test
    public void when_twoInputsIdle_then_singleIdleMessage() {
        wc = WatermarkCoalescer.create(20, 3);
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        wc.observeWm(0, 1, IDLE_MESSAGE.timestamp());
    }

    @Test
    public void when_duplicateDoneCall_then_error() {
        assertEquals(Long.MIN_VALUE, wc.queueDone(0));
        exception.expectMessage("Duplicate");
        assertEquals(Long.MIN_VALUE, wc.queueDone(0));
    }

    @Test
    public void when_wmAfterDone_then_error() {
        assertEquals(Long.MIN_VALUE, wc.queueDone(0));
        exception.expectMessage("not monotonically increasing");
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 0));
    }

    @Test
    public void when_idleMessageAfterDone_then_error() {
        assertEquals(Long.MIN_VALUE, wc.queueDone(0));
        exception.expectMessage("not monotonically increasing");
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
    }

    @Test
    public void when_wmGoesBack_then_error() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 10));
        exception.expectMessage("not monotonically increasing");
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 9));
    }

    @Test
    public void when_noHistory_then_checkHistoryDoesNothing() {
        wc = WatermarkCoalescer.create(-1, 2);
        for (int i = 0; i < 100; i++) {
            assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(i)));
            assertEquals(Long.MIN_VALUE, wc.checkWmHistory(wc.getTime()));
        }
    }

    @Test
    public void when_allInputsHadWms_allBecomeIdle_theLessAheadBecomesIdleLater_then_topWmForwarded() {
        // When
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 10));
        assertEquals(10L, wc.observeWm(0, 1, 11));

        // now queue1 becomes idle. Wm should stay at 10
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 1, IDLE_MESSAGE.timestamp()));

        // Then
        // queue0 becomes idle. Wm should be forwarded to 11
        assertEquals(11L, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        assertEquals(IDLE_MESSAGE.timestamp(), wc.checkWmHistory(0));
    }

    @Test
    public void when_allInputsHadWms_aheadOnesBecomeIdle_behindOneIsDone_then_topWmForwarded() {
        // When
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 10));
        assertEquals(10L, wc.observeWm(0, 1, 11));

        // now queue1 becomes idle. Wm should stay at 10
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 1, IDLE_MESSAGE.timestamp()));

        // Then
        // queue0 becomes done. Wm should be forwarded to 11
        assertEquals(11L, wc.queueDone(0));
        assertEquals(IDLE_MESSAGE.timestamp(), wc.checkWmHistory(0));
    }
}
