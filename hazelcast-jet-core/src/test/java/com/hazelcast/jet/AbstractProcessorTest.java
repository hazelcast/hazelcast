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
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import javax.annotation.Nonnull;

import java.net.UnknownHostException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class AbstractProcessorTest {

    private static final String MOCK_ITEM = "x";
    private static final int OUTBOX_BUCKET_COUNT = 2;

    private RegisteringMethodCallsP p;
    private SpecializedByOrdinalP tryProcessP;

    private ArrayDequeInbox inbox;
    private ArrayDequeOutbox outbox;

    @Before
    public void before() {
        inbox = new ArrayDequeInbox();
        inbox.add(MOCK_ITEM);
        int[] capacities = new int[OUTBOX_BUCKET_COUNT];
        Arrays.fill(capacities, 1);
        outbox = new ArrayDequeOutbox(capacities, new ProgressTracker());
        final Processor.Context ctx = mock(Processor.Context.class);
        Mockito.when(ctx.logger()).thenReturn(mock(ILogger.class));

        p = new RegisteringMethodCallsP();
        p.init(outbox, ctx);
        tryProcessP = new SpecializedByOrdinalP();
        tryProcessP.init(outbox, ctx);
    }

    @Test
    public void when_init_then_customInitCalled() {
        assertTrue(tryProcessP.initCalled);
    }

    @Test
    public void when_init_then_loggerAvailable() {
        // When
        final ILogger logger = p.getLogger();

        // Then
        assertNotNull(logger);
    }

    @Test(expected = UnknownHostException.class)
    public void when_customInitThrows_then_initRethrows() {
        new AbstractProcessor() {
            @Override
            protected void init(@Nonnull Context context) throws Exception {
                throw new UnknownHostException();
            }
        }.init(mock(Outbox.class), mock(Processor.Context.class));
    }

    @Test
    public void when_process0_then_tryProcess0Called() {
        // When
        tryProcessP.process(0, inbox);

        // Then
        tryProcessP.validateReception(0, MOCK_ITEM);
    }

    @Test
    public void when_process1_then_tryProcess1Called() {
        // When
        tryProcessP.process(1, inbox);

        // Then
        tryProcessP.validateReception(1, MOCK_ITEM);
    }

    @Test
    public void when_process2_then_tryProcess2Called() {
        // When
        tryProcessP.process(2, inbox);

        // Then
        tryProcessP.validateReception(2, MOCK_ITEM);
    }

    @Test
    public void when_process3_then_tryProcess3Called() {
        // When
        tryProcessP.process(3, inbox);

        // Then
        tryProcessP.validateReception(3, MOCK_ITEM);
    }

    @Test
    public void when_process4_then_tryProcess4Called() {
        // When
        tryProcessP.process(4, inbox);

        // Then
        tryProcessP.validateReception(4, MOCK_ITEM);
    }

    @Test
    public void when_process5_then_tryProcessCalled() {
        // When
        tryProcessP.process(5, inbox);

        // Then
        tryProcessP.validateReception(5, MOCK_ITEM);
    }

    @Test(expected = UnknownHostException.class)
    public void when_processNThrows_then_processRethrows() {
        // Given
        AbstractProcessor p = new AbstractProcessor() {
            @Override
            protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
                throw new UnknownHostException();
            }
        };
        p.init(mock(Outbox.class), mock(Processor.Context.class));

        // When
        p.process(0, inbox);

        // Then don't reach this line
    }

    @Test(expected = UnsupportedOperationException.class)
    public void when_tryProcessNotOverridden_then_unsupportedOperation() throws Exception {
        new AbstractProcessor() {}.tryProcess(0, MOCK_ITEM);
    }

    @Test
    public void when_tryProcess0NotOverridden_then_delegatesToTryProcess() throws Exception {
        // When
        p.tryProcess0(MOCK_ITEM);

        // Then
        assertEquals(MOCK_ITEM, p.resultOfTryProcessN[0]);
    }

    @Test
    public void when_tryProcess1NotOverridden_then_delegatesToTryProcess() throws Exception {
        p.tryProcess1(MOCK_ITEM);
        assertEquals(MOCK_ITEM, p.resultOfTryProcessN[1]);
    }

    @Test
    public void when_tryProcess2NotOverridden_then_delegatesToTryProcess() throws Exception {
        p.tryProcess2(MOCK_ITEM);
        assertEquals(MOCK_ITEM, p.resultOfTryProcessN[2]);
    }

    @Test
    public void when_tryProcess3NotOverridden_then_delegatesToTryProcess() throws Exception {
        p.tryProcess3(MOCK_ITEM);
        assertEquals(MOCK_ITEM, p.resultOfTryProcessN[3]);
    }

    @Test
    public void when_tryProcess4NotOverridden_then_delegatesToTryProcess() throws Exception {
        p.tryProcess4(MOCK_ITEM);
        assertEquals(MOCK_ITEM, p.resultOfTryProcessN[4]);
    }

    @Test
    public void when_emit_then_outboxHasItemInAllBuckets() {
        // When
        p.tryEmit(MOCK_ITEM);

        // Then
        for (int i = 0; i < OUTBOX_BUCKET_COUNT; i++) {
            assertEquals(MOCK_ITEM, outbox.queueWithOrdinal(i).remove());
        }
    }

    @Test
    public void when_emit1_then_outboxBucket1HasItem() {
        final int ordinal = 1;

        // When
        p.tryEmit(ordinal, MOCK_ITEM);

        // Then
        for (int i = 0; i < OUTBOX_BUCKET_COUNT; i++) {
            assertEquals(i == ordinal ? MOCK_ITEM : null, outbox.queueWithOrdinal(i).poll());
        }
    }

    @Test
    public void when_emitFromTraverser_then_outboxHasOneItem() {
        final int ordinal = 1;

        // When
        p.emitFromTraverser(ordinal, () -> MOCK_ITEM);

        // Then
        for (int i = 0; i < OUTBOX_BUCKET_COUNT; i++) {
            assertEquals(i == ordinal ? MOCK_ITEM : null, outbox.queueWithOrdinal(i).poll());
        }
        assertNull(outbox.queueWithOrdinal(ordinal).poll());
    }

    @Test
    public void when_flatMapperTryProcess_then_outboxHasOneItem() {
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

    private static class RegisteringMethodCallsP extends AbstractProcessor {
        boolean initCalled;
        Object[] resultOfTryProcessN = new Object[6];

        @Override
        protected void init(@Nonnull Context context) {
            initCalled = true;
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            resultOfTryProcessN[ordinal] = item;
            return true;
        }

        void validateReception(int ordinal, Object item) {
            for (int i = 0; i < resultOfTryProcessN.length; i++) {
                assertSame(i == ordinal ? item : null, resultOfTryProcessN[i]);
            }
        }
    }

    private static class SpecializedByOrdinalP extends RegisteringMethodCallsP {

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            assertEquals(5, ordinal);
            return super.tryProcess(ordinal, item);
        }

        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            resultOfTryProcessN[0] = item;
            return true;
        }

        @Override
        protected boolean tryProcess1(@Nonnull Object item) {
            resultOfTryProcessN[1] = item;
            return true;
        }

        @Override
        protected boolean tryProcess2(@Nonnull Object item) {
            resultOfTryProcessN[2] = item;
            return true;
        }

        @Override
        protected boolean tryProcess3(@Nonnull Object item) {
            resultOfTryProcessN[3] = item;
            return true;
        }

        @Override
        protected boolean tryProcess4(@Nonnull Object item) {
            resultOfTryProcessN[4] = item;
            return true;
        }
    }
}
