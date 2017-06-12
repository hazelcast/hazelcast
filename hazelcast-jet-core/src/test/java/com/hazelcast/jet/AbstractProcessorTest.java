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
import java.util.Queue;
import java.util.stream.Stream;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.stream.IntStream.range;
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
    private static final Watermark MOCK_WM = new Watermark(17);
    private static final int OUTBOX_BUCKET_COUNT = 4;
    private static final int ORDINAL_0 = 0;
    private static final int ORDINAL_1 = 1;
    private static final int ORDINAL_2 = 2;
    private static final int ORDINAL_3 = 3;
    private static final int ORDINAL_4 = 4;
    private static final int ORDINAL_5 = 5;
    private static final int[] ORDINALS_1_2 = {1, 2};
    private static final int[] ALL_ORDINALS = range(0, OUTBOX_BUCKET_COUNT).toArray();

    private RegisteringMethodCallsP p;
    private SpecializedByOrdinalP tryProcessP;

    private ArrayDequeInbox inbox;
    private ArrayDequeOutbox outbox;
    private NothingOverriddenP nothingOverriddenP;

    @Before
    public void before() {
        inbox = new ArrayDequeInbox();
        inbox.add(MOCK_ITEM);
        inbox.add(MOCK_WM);
        int[] capacities = new int[OUTBOX_BUCKET_COUNT];
        Arrays.fill(capacities, 1);
        outbox = new ArrayDequeOutbox(capacities, new ProgressTracker());
        final Processor.Context ctx = mock(Processor.Context.class);
        Mockito.when(ctx.logger()).thenReturn(mock(ILogger.class));

        p = new RegisteringMethodCallsP();
        p.init(outbox, ctx);
        tryProcessP = new SpecializedByOrdinalP();
        tryProcessP.init(outbox, ctx);
        nothingOverriddenP = new NothingOverriddenP();
        nothingOverriddenP.init(outbox, ctx);
    }

    @Test
    public void when_setCooperative_then_isCooperative() {
        Stream.of(FALSE, TRUE).forEach(b -> {
            // When
            p.setCooperative(b);

            // Then
            assertEquals(b, p.isCooperative());
        });
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
            protected void init(@Nonnull Context context) throws UnknownHostException {
                throw new UnknownHostException();
            }
        }.init(mock(Outbox.class), mock(Processor.Context.class));
    }

    @Test
    public void when_processInbox0_then_tryProcess0Called() {
        // When
        tryProcessP.process(ORDINAL_0, inbox);

        // Then
        tryProcessP.validateReceptionOfItem(ORDINAL_0, MOCK_ITEM);
    }

    @Test
    public void when_processInbox1_then_tryProcess1Called() {
        // When
        tryProcessP.process(ORDINAL_1, inbox);

        // Then
        tryProcessP.validateReceptionOfItem(ORDINAL_1, MOCK_ITEM);
    }

    @Test
    public void when_processInbox2_then_tryProcess2Called() {
        // When
        tryProcessP.process(ORDINAL_2, inbox);

        // Then
        tryProcessP.validateReceptionOfItem(ORDINAL_2, MOCK_ITEM);
    }

    @Test
    public void when_processInbox3_then_tryProcess3Called() {
        // When
        tryProcessP.process(ORDINAL_3, inbox);

        // Then
        tryProcessP.validateReceptionOfItem(ORDINAL_3, MOCK_ITEM);
    }

    @Test
    public void when_processInbox4_then_tryProcess4Called() {
        // When
        tryProcessP.process(ORDINAL_4, inbox);

        // Then
        tryProcessP.validateReceptionOfItem(ORDINAL_4, MOCK_ITEM);
    }

    @Test
    public void when_processInbox5_then_tryProcessCalled() {
        // When
        tryProcessP.process(ORDINAL_5, inbox);

        // Then
        tryProcessP.validateReceptionOfItem(ORDINAL_5, MOCK_ITEM);
    }

    @Test(expected = UnknownHostException.class)
    public void when_processNThrows_then_processRethrows() {
        // Given
        AbstractProcessor p = new AbstractProcessor() {
            @Override
            protected boolean tryProcess(int ordinal, @Nonnull Object item) throws UnknownHostException {
                throw new UnknownHostException();
            }
        };
        p.init(mock(Outbox.class), mock(Processor.Context.class));

        // When
        p.process(ORDINAL_0, inbox);

        // Then don't reach this line
    }

    @Test(expected = UnsupportedOperationException.class)
    public void when_tryProcess_then_unsupportedOperation() throws Exception {
        nothingOverriddenP.tryProcess(ORDINAL_0, MOCK_ITEM);
    }

    @Test
    public void when_tryProcess0_then_delegatesToTryProcess() throws Exception {
        // When
        boolean done = p.tryProcess0(MOCK_ITEM);

        // Then
        assertTrue(done);
        p.validateReceptionOfItem(ORDINAL_0, MOCK_ITEM);
    }

    @Test
    public void when_tryProcess1_then_delegatesToTryProcess() throws Exception {
        // When
        boolean done = p.tryProcess1(MOCK_ITEM);

        // Then
        assertTrue(done);
        p.validateReceptionOfItem(ORDINAL_1, MOCK_ITEM);
    }

    @Test
    public void when_tryProcess2_then_delegatesToTryProcess() throws Exception {
        // When
        boolean done = p.tryProcess2(MOCK_ITEM);

        // Then
        assertTrue(done);
        p.validateReceptionOfItem(ORDINAL_2, MOCK_ITEM);
    }

    @Test
    public void when_tryProcess3_then_delegatesToTryProcess() throws Exception {
        // When
        boolean done = p.tryProcess3(MOCK_ITEM);

        // Then
        assertTrue(done);
        p.validateReceptionOfItem(ORDINAL_3, MOCK_ITEM);
    }

    @Test
    public void when_tryProcess4_then_delegatesToTryProcess() throws Exception {
        // When
        boolean done = p.tryProcess4(MOCK_ITEM);

        // Then
        assertTrue(done);
        p.validateReceptionOfItem(ORDINAL_4, MOCK_ITEM);
    }

    @Test
    public void when_tryProcessWm_then_passesOnWm() {
        // When
        boolean done = nothingOverriddenP.tryProcessWm(ORDINAL_0, MOCK_WM);

        // Then
        assertTrue(done);
        validateReceptionAtOrdinals(MOCK_WM, ALL_ORDINALS);
    }

    @Test
    public void when_tryProcessWm0_then_delegatesToTryProcessWm() throws Exception {
        // When
        boolean done = p.tryProcessWm0(MOCK_WM);

        // Then
        assertTrue(done);
        p.validateReceptionOfWm(ORDINAL_0, MOCK_WM);
    }

    @Test
    public void when_tryProcessWm1_then_delegatesToTryProcessWm() throws Exception {
        // When
        boolean done = p.tryProcessWm1(MOCK_WM);

        // Then
        assertTrue(done);
        p.validateReceptionOfWm(ORDINAL_1, MOCK_WM);
    }

    @Test
    public void when_tryProcessWm2_then_delegatesToTryProcessWm() throws Exception {
        // When
        boolean done = p.tryProcessWm2(MOCK_WM);

        // Then
        assertTrue(done);
        p.validateReceptionOfWm(ORDINAL_2, MOCK_WM);
    }

    @Test
    public void when_tryProcessWm3_then_delegatesToTryProcessWm() throws Exception {
        // When
        boolean done = p.tryProcessWm3(MOCK_WM);

        // Then
        assertTrue(done);
        p.validateReceptionOfWm(ORDINAL_3, MOCK_WM);
    }

    @Test
    public void when_tryProcessWm4_then_delegatesToTryProcessWm() throws Exception {
        // When
        boolean done = p.tryProcessWm4(MOCK_WM);

        // Then
        assertTrue(done);
        p.validateReceptionOfWm(ORDINAL_4, MOCK_WM);
    }

    @Test
    public void when_process0ButOutboxFull_then_itemNotRemoved() throws Exception {
        // Given
        resetInboxToTwoWms();

        // When
        nothingOverriddenP.process0(inbox);

        // Then
        assertEquals(MOCK_WM, inbox.poll());
    }

    @Test
    public void when_process1ButOutboxFull_then_itemNotRemoved() throws Exception {
        // Given
        resetInboxToTwoWms();

        // When
        nothingOverriddenP.process1(inbox);

        // Then
        assertEquals(MOCK_WM, inbox.poll());
    }

    @Test
    public void when_process2ButOutboxFull_then_itemNotRemoved() throws Exception {
        // Given
        resetInboxToTwoWms();

        // When
        nothingOverriddenP.process2(inbox);

        // Then
        assertEquals(MOCK_WM, inbox.poll());
    }

    @Test
    public void when_process3ButOutboxFull_then_itemNotRemoved() throws Exception {
        // Given
        resetInboxToTwoWms();

        // When
        nothingOverriddenP.process3(inbox);

        // Then
        assertEquals(MOCK_WM, inbox.poll());
    }

    @Test
    public void when_process4ButOutboxFull_then_itemNotRemoved() throws Exception {
        // Given
        resetInboxToTwoWms();

        // When
        nothingOverriddenP.process4(inbox);

        // Then
        assertEquals(MOCK_WM, inbox.poll());
    }

    @Test
    public void when_processAnyButOutboxFull_then_itemNotRemoved() throws Exception {
        // Given
        resetInboxToTwoWms();

        // When
        nothingOverriddenP.processAny(5, inbox);

        // Then
        assertEquals(MOCK_WM, inbox.poll());
    }

    private void resetInboxToTwoWms() {
        inbox.clear();
        inbox.add(MOCK_WM);
        inbox.add(MOCK_WM);
    }

    @Test
    public void when_tryEmitToAll_then_emittedToAll() {
        // When
        boolean emitted = p.tryEmit(MOCK_ITEM);

        // Then
        assertTrue(emitted);
        validateReceptionAtOrdinals(MOCK_ITEM, ALL_ORDINALS);
    }

    @Test
    public void when_tryEmitTo1_then_emittedTo1() {
        // When
        boolean emitted = p.tryEmit(ORDINAL_1, MOCK_ITEM);

        // Then
        assertTrue(emitted);
        validateReceptionAtOrdinals(MOCK_ITEM, ORDINAL_1);
    }

    @Test
    public void when_tryEmitTo1And2_then_emittedTo1And2() {
        // When
        boolean emitted = p.tryEmit(ORDINALS_1_2, MOCK_ITEM);

        // Then
        assertTrue(emitted);
        validateReceptionAtOrdinals(MOCK_ITEM, ORDINALS_1_2);
    }

    @Test
    public void when_emitToAll_then_emittedToAll() {
        // When
        p.emit(MOCK_ITEM);

        // Then
        validateReceptionAtOrdinals(MOCK_ITEM, ALL_ORDINALS);
    }

    @Test
    public void when_emitTo1_then_emittedTo1() {
        // When
        p.emit(ORDINAL_1, MOCK_ITEM);

        // Then
        validateReceptionAtOrdinals(MOCK_ITEM, ORDINAL_1);
    }

    @Test
    public void when_emitTo1And2_then_emittedTo1And2() {
        // When
        p.emit(ORDINALS_1_2, MOCK_ITEM);

        // Then
        validateReceptionAtOrdinals(MOCK_ITEM, ORDINALS_1_2);
    }

    @Test(expected = IllegalStateException.class)
    public void when_emitToFullOutbox_then_exception() {
        // Given -- fills outbox
        p.emit(MOCK_ITEM);

        // When
        p.emit(MOCK_ITEM);

        // Then don't reach this line
    }

    @Test
    public void when_emitFromTraverserToAll_then_emittedToAll() {
        // Given
        Traverser<Object> trav = Traverser.over(MOCK_ITEM);

        // When
        boolean done = p.emitFromTraverser(trav);

        // Then
        assertTrue(done);
        validateReceptionAtOrdinals(MOCK_ITEM, ALL_ORDINALS);
    }

    @Test
    public void when_emitFromTraverserTo1_then_emittedTo1() {
        // Given
        Traverser<Object> trav = Traverser.over(MOCK_ITEM, MOCK_ITEM);

        boolean done;
        do {
            // When
            done = p.emitFromTraverser(ORDINAL_1, trav);
            // Then
            validateReceptionAtOrdinals(MOCK_ITEM, ORDINAL_1);
        } while (!done);
    }

    @Test
    public void when_emitFromTraverserTo1And2_then_emittedTo1And2() {
        // Given
        Traverser<Object> trav = Traverser.over(MOCK_ITEM, MOCK_ITEM);

        boolean done;
        do {
            // When
            done = p.emitFromTraverser(ORDINALS_1_2, trav);
            // Then
            validateReceptionAtOrdinals(MOCK_ITEM, ORDINALS_1_2);
        } while (!done);
    }

    @Test
    public void when_flatMapperTryProcessTo1And2_then_emittedTo1And2() {
        final Object item1 = 1;
        final Object item2 = 2;
        final int[] ordinals = {1, 2};
        final FlatMapper<String, Object> flatMapper = p.flatMapper(ordinals, x -> Traverser.over(item1, item2));

        // When
        boolean done = flatMapper.tryProcess(MOCK_ITEM);

        // Then
        assertFalse(done);
        validateReceptionAtOrdinals(item1, ordinals);

        // When
        done = flatMapper.tryProcess(MOCK_ITEM);

        // Then
        assertTrue(done);
        validateReceptionAtOrdinals(item2, ordinals);
    }

    @Test
    public void when_flatMapperTo1_then_emittedTo1() {
        // Given
        Object output = 42;
        FlatMapper<Object, Object> m = p.flatMapper(ORDINAL_1, x -> Traverser.over(output));

        // When
        boolean done = m.tryProcess(MOCK_ITEM);

        // Then
        assertTrue(done);
        validateReceptionAtOrdinals(output, ORDINAL_1);
    }

    @Test
    public void when_flatMapperToAll_then_emittedToAll() {
        // Given
        Object output = 42;
        FlatMapper<Object, Object> m = p.flatMapper(x -> Traverser.over(output));

        // When
        boolean done = m.tryProcess(MOCK_ITEM);

        // Then
        assertTrue(done);
        validateReceptionAtOrdinals(output, ALL_ORDINALS);
    }

    private void validateReceptionAtOrdinals(Object item, int... ordinals) {
        for (int i : range(0, OUTBOX_BUCKET_COUNT).toArray()) {
            Queue<Object> q = outbox.queueWithOrdinal(i);
            if (Arrays.stream(ordinals).anyMatch(ord -> ord == i)) {
                assertEquals(item, q.poll());
            }
            assertNull(q.poll());
        }
    }

    private static class RegisteringMethodCallsP extends AbstractProcessor {
        boolean initCalled;
        Object[] receivedByTryProcessN = new Object[6];
        Object[] receivedByTryProcessWmN = new Object[6];

        @Override
        protected void init(@Nonnull Context context) {
            initCalled = true;
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            receivedByTryProcessN[ordinal] = item;
            return true;
        }

        @Override
        protected boolean tryProcessWm(int ordinal, @Nonnull Watermark wm) {
            receivedByTryProcessWmN[ordinal] = wm;
            return true;
        }

        void validateReceptionOfItem(int ordinal, Object item) {
            for (int i = 0; i < receivedByTryProcessN.length; i++) {
                assertSame(i == ordinal ? item : null, receivedByTryProcessN[i]);
            }
        }

        void validateReceptionOfWm(int ordinal, Watermark wm) {
            for (int i = 0; i < receivedByTryProcessWmN.length; i++) {
                assertSame(i == ordinal ? wm : null, receivedByTryProcessWmN[i]);
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
            receivedByTryProcessN[0] = item;
            return true;
        }

        @Override
        protected boolean tryProcess1(@Nonnull Object item) {
            receivedByTryProcessN[1] = item;
            return true;
        }

        @Override
        protected boolean tryProcess2(@Nonnull Object item) {
            receivedByTryProcessN[2] = item;
            return true;
        }

        @Override
        protected boolean tryProcess3(@Nonnull Object item) {
            receivedByTryProcessN[3] = item;
            return true;
        }

        @Override
        protected boolean tryProcess4(@Nonnull Object item) {
            receivedByTryProcessN[4] = item;
            return true;
        }
    }

    private static class NothingOverriddenP extends AbstractProcessor {
    }
}
