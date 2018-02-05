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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.spi.serialization.SerializationService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.function.Predicate;

import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class OutboxImplTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private OutboxImpl outbox = new OutboxImpl(new OutboundCollector[] {e -> DONE, e -> DONE},
            true, new ProgressTracker(), mock(SerializationService.class), 3);

    @Before
    public void before() {
        outbox.reset();
    }

    @Test
    public void when_offer1_then_rateLimited() {
        do_when_offer_then_rateLimited(e -> outbox.offer(e));
    }

    @Test
    public void when_offer2_then_rateLimited() {
        do_when_offer_then_rateLimited(e -> outbox.offer(0, e));
    }

    @Test
    public void when_offer3_then_rateLimited() {
        do_when_offer_then_rateLimited(e -> outbox.offerToSnapshot(e, e));
    }

    @Test
    public void when_offer4_then_rateLimited() {
        do_when_offer_then_rateLimited(e -> outbox.offer(new int[] {0}, e));
    }

    @Test
    public void when_offer5_then_rateLimited() {
        do_when_offer_then_rateLimited(e -> outbox.offerToEdgesAndSnapshot(e));
    }

    @Test
    public void when_queueFullAndOfferReturnedFalse_then_subsequentCallFails() {
        // See https://github.com/hazelcast/hazelcast-jet/issues/622
        outbox = new OutboxImpl(new OutboundCollector[] {e -> DONE, e -> NO_PROGRESS},
                true, new ProgressTracker(), mock(SerializationService.class), 128);

        // we succeed offering to one queue, but not to the other, thus false
        assertFalse(outbox.offer(4));

        // Then
        exception.expectMessage("offer() called again");
        outbox.offer(4);
    }

    @Test
    public void when_batchSizeReachedAndOfferReturnedFalse_then_subsequentCallFails() {
        // See https://github.com/hazelcast/hazelcast-jet/issues/622
        assertTrue(outbox.offer(1));
        assertTrue(outbox.offer(2));
        assertTrue(outbox.offer(3));
        assertFalse(outbox.offer(4));

        // Then
        try {
            outbox.offer(4);
            fail("expected exception not thrown");
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("offer() called again"));
        }

        // let's reset the outbox, we should be able to offer now
        outbox.reset();
        assertTrue(outbox.offer(4));
    }

    @Test
    public void when_sameItemOfferedTwice_then_success() {
        String item = "foo";
        assertTrue(outbox.offer(item));
        assertTrue(outbox.offer(item));
    }

    private void do_when_offer_then_rateLimited(Predicate<Object> offerF) {
        assertTrue(offerF.test(1));
        assertTrue(offerF.test(2));
        assertTrue(offerF.test(3));
        assertFalse(offerF.test(4));
    }
}
