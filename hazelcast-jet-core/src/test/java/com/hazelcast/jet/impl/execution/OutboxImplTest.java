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
import org.junit.Test;

import java.util.function.Predicate;

import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class OutboxImplTest {

    private OutboxImpl outbox = new OutboxImpl(new OutboundCollector[] {e -> DONE, e -> DONE},
            true, new ProgressTracker(), mock(SerializationService.class), 3);

    @Before
    public void before() {
        outbox.resetBatch();
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
        do_when_offer_then_rateLimited(e -> outbox.offerToSnapshot("key", "value"));
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

}
