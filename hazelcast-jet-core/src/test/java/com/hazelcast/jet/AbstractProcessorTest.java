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

package com.hazelcast.jet;

import com.hazelcast.jet.AbstractProcessor.FlatMapper;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.logging.ILogger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nonnull;

import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class AbstractProcessorTest {

    private static final String MOCK_ITEM = "x";
    private static final int OUTBOX_BUCKET_COUNT = 2;

    private SpecializedByOrdinal p;

    private ArrayDequeOutbox outbox;

    @Before
    public void before() {
        p = new SpecializedByOrdinal();
        final Processor.Context ctx = mock(Processor.Context.class);
        Mockito.when(ctx.logger()).thenReturn(mock(ILogger.class));
        outbox = new ArrayDequeOutbox(OUTBOX_BUCKET_COUNT, new int[]{1, 1});
        p.init(outbox, ctx);
    }

    @Test
    public void when_init_then_customInitCalled() {
        assertTrue(p.initCalled);
    }

    @Test
    public void when_init_then_outboxAndLoggerAvailable() {
        // When
        final ILogger logger = p.getLogger();
        final Outbox outbox = p.getOutbox();

        // Then
        assertNotNull(logger);
        assertNotNull(outbox);
    }

    @Test
    public void when_process0_then_tryProcess0Called() {
        // When
        p.process(0, new MockInbox(MOCK_ITEM));

        // Then
        p.validateReception(0, MOCK_ITEM);
    }

    @Test
    public void when_process1_then_tryProcess1Called() {
        // When
        p.process(1, new MockInbox(MOCK_ITEM));

        // Then
        p.validateReception(1, MOCK_ITEM);
    }

    @Test
    public void when_process2_then_tryProcess2Called() {
        // When
        p.process(2, new MockInbox(MOCK_ITEM));

        // Then
        p.validateReception(2, MOCK_ITEM);
    }

    @Test
    public void when_process3_then_tryProcess3Called() {
        // When
        p.process(3, new MockInbox(MOCK_ITEM));

        // Then
        p.validateReception(3, MOCK_ITEM);
    }

    @Test
    public void when_process4_then_tryProcess4Called() {
        // When
        p.process(4, new MockInbox(MOCK_ITEM));

        // Then
        p.validateReception(4, MOCK_ITEM);
    }

    @Test
    public void when_process5_then_tryProcessCalled() {
        // When
        p.process(5, new MockInbox(MOCK_ITEM));

        // Then
        p.validateReception(5, MOCK_ITEM);
    }

    @Test
    public void when_emit_then_outboxHasItemInAllBuckets() {
        // When
        p.emit(MOCK_ITEM);

        // Then
        for (int i = 0; i < OUTBOX_BUCKET_COUNT; i++) {
            assertEquals(MOCK_ITEM, ((ArrayDequeOutbox) p.getOutbox()).queueWithOrdinal(i).remove());
        }
    }

    @Test
    public void when_emit1_then_outboxBucket1HasItem() {
        final int ordinal = 1;

        // When
        p.emit(ordinal, MOCK_ITEM);

        // Then
        for (int i = 0; i < OUTBOX_BUCKET_COUNT; i++) {
            assertEquals(i == ordinal ? MOCK_ITEM : null,
                    ((ArrayDequeOutbox) p.getOutbox()).queueWithOrdinal(i).poll());
        }
    }

    @Test
    public void when_emitCooperatively_then_outboxHasOneItem() {
        final int ordinal = 1;
        final ArrayDequeOutbox outbox = (ArrayDequeOutbox) p.getOutbox();

        // When
        p.emitCooperatively(ordinal, () -> MOCK_ITEM);

        // Then
        for (int i = 0; i < OUTBOX_BUCKET_COUNT; i++) {
            assertEquals(i == ordinal ? MOCK_ITEM : null, outbox.queueWithOrdinal(i).poll());
        }
        assertNull(outbox.queueWithOrdinal(ordinal).poll());
    }

    @Test
    public void test() {
        final Object item1 = 1;
        final Object item2 = 2;
        final FlatMapper<String, Object> flatMapper = p.flatMapper(x -> Traverser.over(item1, item2));

        // When
        boolean done = flatMapper.tryProcess(MOCK_ITEM);

        // Then
        assertFalse(done);
        for (int i = 0; i < OUTBOX_BUCKET_COUNT; i++) {
            assertEquals(item1, outbox.queueWithOrdinal(i).poll());
            assertNull(outbox.queueWithOrdinal(i).poll());
        }

        // When
        done = flatMapper.tryProcess(MOCK_ITEM);

        // Then
        assertTrue(done);
        for (int i = 0; i < OUTBOX_BUCKET_COUNT; i++) {
            assertEquals(item2, outbox.queueWithOrdinal(i).poll());
            assertNull(outbox.queueWithOrdinal(i).poll());
        }
    }

    private static class SpecializedByOrdinal extends AbstractProcessor {
        boolean initCalled;
        Object[] tryProcessNItem = new Object[6];

        @Override
        protected void init(@Nonnull Context context) throws Exception {
            initCalled = true;
        }

        @Override
        protected boolean tryProcess0(@Nonnull Object MOCK_ITEM) throws Exception {
            tryProcessNItem[0] = MOCK_ITEM;
            return true;
        }

        @Override
        protected boolean tryProcess1(@Nonnull Object MOCK_ITEM) throws Exception {
            tryProcessNItem[1] = MOCK_ITEM;
            return true;
        }

        @Override
        protected boolean tryProcess2(@Nonnull Object MOCK_ITEM) throws Exception {
            tryProcessNItem[2] = MOCK_ITEM;
            return true;
        }

        @Override
        protected boolean tryProcess3(@Nonnull Object MOCK_ITEM) throws Exception {
            tryProcessNItem[3] = MOCK_ITEM;
            return true;
        }

        @Override
        protected boolean tryProcess4(@Nonnull Object MOCK_ITEM) throws Exception {
            tryProcessNItem[4] = MOCK_ITEM;
            return true;
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
            assertEquals(5, ordinal);
            tryProcessNItem[5] = item;
            return true;
        }

        void validateReception(int ordinal, Object MOCK_ITEM) {
            for (int i = 0; i < tryProcessNItem.length; i++) {
                assertEquals(i == ordinal ? MOCK_ITEM : null, tryProcessNItem[i]);
            }
        }
    }

    private static final class MockInbox implements Inbox {

        private Object MOCK_ITEM;

        private MockInbox(Object MOCK_ITEM) {
            this.MOCK_ITEM = MOCK_ITEM;
        }

        @Override
        public boolean isEmpty() {
            return MOCK_ITEM == null;
        }

        @Override
        public Object peek() {
            return MOCK_ITEM;
        }

        @Override
        public Object poll() {
            try {
                return MOCK_ITEM;
            } finally {
                MOCK_ITEM = null;
            }
        }

        @Override
        public Object remove() {
            if (MOCK_ITEM == null) {
                throw new NoSuchElementException();
            }
            try {
                return MOCK_ITEM;
            } finally {
                MOCK_ITEM = null;
            }
        }
    }
}
