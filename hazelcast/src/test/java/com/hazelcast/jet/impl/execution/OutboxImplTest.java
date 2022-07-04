/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Predicate;

import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OutboxImplTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private OutboxImpl outbox = new OutboxImpl(new OutboundCollector[] {e -> DONE, e -> DONE, e -> DONE},
            true, new ProgressTracker(), mockSerializationService(), 3, new AtomicLongArray(4));

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

    private void do_when_offer_then_rateLimited(Predicate<Object> offerF) {
        assertTrue(offerF.test(1));
        assertTrue(offerF.test(2));
        assertTrue(offerF.test(3));
        assertFalse(offerF.test(4));
    }

    @Test
    public void when_queueFullAndOfferReturnedFalse_then_subsequentCallFails() {
        // See https://github.com/hazelcast/hazelcast-jet/issues/622
        outbox = new OutboxImpl(new OutboundCollector[] {e -> DONE, e -> NO_PROGRESS},
                true, new ProgressTracker(), mockSerializationService(), 128, new AtomicLongArray(3));

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

    @Test
    public void when_offer1FailsAndDifferentItemOffered_then_fail() {
        do_when_offerDifferent_then_fail(e -> outbox.offer(e));
    }

    @Test
    public void when_offer2FailsAndDifferentItemOffered_then_fail() {
        do_when_offerDifferent_then_fail(e -> outbox.offer(0, e));
    }

    @Test
    public void when_offer3FailsAndDifferentItemOffered_then_fail() {
        do_when_offerDifferent_then_fail(e -> outbox.offerToSnapshot(e, e));
    }

    @Test
    public void when_offer4FailsAndDifferentItemOffered_then_fail() {
        do_when_offerDifferent_then_fail(e -> outbox.offer(new int[] {0}, e));
    }

    @Test
    public void when_offer5FailsAndDifferentItemOffered_then_fail() {
        do_when_offerDifferent_then_fail(e -> outbox.offerToEdgesAndSnapshot(e));
    }

    @Test
    public void when_offerFailsAndOfferedToDifferentOrdinal_then_fail_1() {
        do_when_offerToDifferentOrdinal_then_fail(e -> outbox.offer(0, e), e -> outbox.offer(1, e));
    }

    @Test
    public void when_offerFailsAndOfferedToDifferentOrdinal_then_fail_2() {
        do_when_offerToDifferentOrdinal_then_fail(e -> outbox.offer(0, e), e -> outbox.offerToSnapshot(e, e));
    }

    @Test
    public void when_offerFailsAndOfferedToDifferentOrdinal_then_fail_3() {
        do_when_offerToDifferentOrdinal_then_fail(e -> outbox.offer(0, e), e -> outbox.offer(e));
    }

    @Test
    public void when_offerFailsAndOfferedToDifferentOrdinal_then_fail_4() {
        do_when_offerToDifferentOrdinal_then_fail(e -> outbox.offer(0, e), e -> outbox.offerToEdgesAndSnapshot(e));
    }

    @Test
    public void when_blocked_then_allowsOnlyFinishingTheItem() {
        boolean[] allowOffer = {false};
        assertFalse(outbox.hasUnfinishedItem());
        outbox = new OutboxImpl(new OutboundCollector[] {e -> allowOffer[0] ? DONE : NO_PROGRESS},
                true, new ProgressTracker(), mockSerializationService(), 128, new AtomicLongArray(3));

        assertFalse(outbox.offer(4));
        assertTrue(outbox.hasUnfinishedItem());
        // When
        outbox.block();
        outbox.reset();
        allowOffer[0] = true;
        // Then
        assertTrue(outbox.offer(4));
        assertFalse(outbox.hasUnfinishedItem());
        assertFalse(outbox.offer(5));
        assertFalse(outbox.hasUnfinishedItem());

        outbox.unblock();
        assertTrue(outbox.offer(5));
    }

    private void do_when_offerDifferent_then_fail(Predicate<Object> offerF) {
        assertTrue(offerF.test(1));
        assertTrue(offerF.test(2));
        assertTrue(offerF.test(3));
        assertFalse(offerF.test(4));

        exception.expect(AssertionError.class);
        exception.expectMessage("Different");
        // we offer the different item than the last false-returning call
        offerF.test(5);
    }

    private void do_when_offerToDifferentOrdinal_then_fail(Predicate<Object> offerF1, Predicate<Object> offerF2) {
        assertTrue(offerF1.test(1));
        assertTrue(offerF1.test(2));
        assertTrue(offerF1.test(3));
        assertFalse(offerF1.test(4));

        exception.expect(AssertionError.class);
        exception.expectMessage("ifferent");
        // we offer the same item as the last false-returning call, but to different offer function
        offerF2.test(4);
    }

    private static SerializationService mockSerializationService() {
        SerializationService mock = mock(SerializationService.class);
        when(mock.toData(any())).thenReturn(mock(Data.class));
        return mock;
    }
}
