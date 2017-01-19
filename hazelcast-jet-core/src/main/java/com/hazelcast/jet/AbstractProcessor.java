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

import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * Base class to implement custom processors. Simplifies the contract of
 * {@code Processor} with several levels of convenience:
 * <ol><li>
 *     {@link #init(Outbox, Context)} retains the supplied outbox and context.
 * </li><li>
 *     {@link #process(int, Inbox)} delegates to {@link #tryProcess(int, Object)}
 *     with each item received in the inbox.
 * </li><li>
 *     The {@code emit(...)} methods avoid the need to deal with {@code Outbox}
 *     directly.
 * </li><li>
 *     The {@code emitCooperatively(...)} methods handle the boilerplate of
 *     cooperative item emission. They are especially useful in the {@link #complete()}
 *     step when there is a collection of items to emit. The {@link Traversers}
 *     class contains traversers tailored to simplify the implementation of
 *     {@code complete()}.
 * </li><li>
 *     The {@link TryProcessor TryProcessor} class additionally simplifies the
 *     usage of {@code emitCooperatively()} inside {@code tryProcess()}, in a
 *     scenario where an input item results in a collection of output items.
 *     {@code TryProcessor} is obtained from its factory method
 *     {@link #tryProcessor(Function)}.
 * </li></ol>
 */
public abstract class AbstractProcessor implements Processor {

    private Outbox outbox;
    private Object pendingItem;
    private ILogger logger;

    @Override
    public final void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        this.outbox = outbox;
        this.logger = context.logger();
        init(context);
    }

    /**
     * Method that can be overridden to perform any necessary initialization for
     * the processor. It is called exactly once and strictly before any of the
     * processing methods ({@link #process(int, Inbox) process()},
     * {@link #completeEdge(int) completeEdge()}, {@link #complete() complete()}),
     * but after the {@link #getOutbox() outbox} and {@link #getLogger() logger}
     * have been initialized.
     *
     * @param context the {@link Context context} associated with this processor
     */
    protected void init(@Nonnull Context context) {
    }

    /**
     * Returns the logger associated with this processor instance.
     */
    protected final ILogger getLogger() {
        return logger;
    }

    /**
     * Implements the boilerplate of dispatching against the ordinal,
     * taking items from the inbox one by one, and invoking the
     * processing logic on each.
     */
    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void process(int ordinal, @Nonnull Inbox inbox) {
        switch (ordinal) {
            case 0:
                process0(inbox);
                return;
            case 1:
                process1(inbox);
                return;
            case 2:
                process2(inbox);
                return;
            case 3:
                process3(inbox);
                return;
            case 4:
                process4(inbox);
                return;
            default:
                processAny(ordinal, inbox);
        }
    }

    /**
     * Tries to process the supplied input item, which was received over
     * the supplied ordinal. May choose to process only partially and return
     * {@code false}, in which case it will be called again later with the
     * same {@code (ordinal, item)} combination.
     * <p>
     * The default implementation throws an {@code UnsupportedOperationException}.
     * <p>
     * <strong>NOTE:</strong> unless the processor doesn't differentiate
     * between its inbound edges, the first choice should be leaving this method
     * alone and instead overriding the specific {@code tryProcessN()} methods
     * for each ordinal the processor expects.
     *
     * @param ordinal ordinal of the edge that delivered the item
     * @param item    item to be processed
     * @return {@code true} if this item has now been processed,
     *         {@code false} otherwise.
     */
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        throw new UnsupportedOperationException("Missing implementation");
    }

    /**
     * Tries to process the supplied input item, which was received from the
     * edge with ordinal 0. May choose to process only partially and return
     * {@code false}, in which case it will be called again later with the same
     * item.
     * <p>
     * The default implementation delegates to
     * {@link #tryProcess(int, Object) tryProcess(0, item)}.
     *
     * @param item    item to be processed
     * @return {@code true} if this item has now been processed,
     *         {@code false} otherwise.
     */
    protected boolean tryProcess0(Object item) {
        return tryProcess(0, item);
    }

    /**
     * Tries to process the supplied input item, which was received from the
     * edge with ordinal 1. May choose to process only partially and return
     * {@code false}, in which case it will be called again later with the same
     * item.
     * <p>
     * The default implementation delegates to
     * {@link #tryProcess(int, Object) tryProcess(1, item)}.
     *
     * @param item    item to be processed
     * @return {@code true} if this item has now been processed,
     *         {@code false} otherwise.
     */
    protected boolean tryProcess1(Object item) {
        return tryProcess(1, item);
    }

    /**
     * Tries to process the supplied input item, which was received from the
     * edge with ordinal 2. May choose to process only partially and return
     * {@code false}, in which case it will be called again later with the same
     * item.
     * <p>
     * The default implementation delegates to
     * {@link #tryProcess(int, Object) tryProcess(2, item)}.
     *
     * @param item    item to be processed
     * @return {@code true} if this item has now been processed,
     *         {@code false} otherwise.
     */
    protected boolean tryProcess2(Object item) {
        return tryProcess(2, item);
    }

    /**
     * Tries to process the supplied input item, which was received from the
     * edge with ordinal 3. May choose to process only partially and return
     * {@code false}, in which case it will be called again later with the same
     * item.
     * <p>
     * The default implementation delegates to
     * {@link #tryProcess(int, Object) tryProcess(3, item)}.
     *
     * @param item    item to be processed
     * @return {@code true} if this item has now been processed,
     *         {@code false} otherwise.
     */
    protected boolean tryProcess3(Object item) {
        return tryProcess(3, item);
    }

    /**
     * Tries to process the supplied input item, which was received from the
     * edge with ordinal 4. May choose to process only partially and return
     * {@code false}, in which case it will be called again later with the same
     * item.
     * <p>
     * The default implementation delegates to
     * {@link #tryProcess(int, Object) tryProcess(4, item)}.
     *
     * @param item    item to be processed
     * @return {@code true} if this item has now been processed,
     *         {@code false} otherwise.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    protected boolean tryProcess4(Object item) {
        return tryProcess(4, item);
    }

    /**
     * Returns the outbox received in the {@code init()} method call.
     */
    protected final Outbox getOutbox() {
        return outbox;
    }

    /**
     * Emits the item to the outbox bucket at the supplied ordinal.
     */
    protected void emit(int ordinal, @Nonnull Object item) {
        outbox.add(ordinal, item);
    }

    /**
     * Emits the item to all the outbox buckets.
     */
    protected void emit(@Nonnull Object item) {
        outbox.add(item);
    }

    /**
     * Emits the items obtained from the traverser to the outbox bucket with the
     * supplied ordinal, in a cooperative fashion: if the outbox reports a
     * high-water condition, backs off and returns {@code false}.
     * <p>
     * If this method returns {@code false}, then the same traverser must be
     * retained by the caller and passed again in the subsequent invocation of
     * this method, so as to resume emitting where it left off.
     *
     * @param ordinal ordinal of the target bucket
     * @param traverser traverser over items to emit
     * @return whether the traverser has been exhausted
     */
    protected boolean emitCooperatively(int ordinal, @Nonnull Traverser<?> traverser) {
        Object item;
        if (pendingItem != null) {
            item = pendingItem;
            pendingItem = null;
        } else {
            item = traverser.next();
        }
        for (; item != null; item = traverser.next()) {
            if (outbox.isHighWater(ordinal)) {
                pendingItem = item;
                return false;
            }
            emit(ordinal, item);
        }
        return true;
    }

    /**
     * Convenience for {@link #emitCooperatively(int, Traverser)} which emits to all ordinals.
     */
    protected boolean emitCooperatively(@Nonnull Traverser<?> traverser) {
        return emitCooperatively(-1, traverser);
    }

    /**
     * Factory of {@link TryProcessor}s.
     */
    @Nonnull
    protected <T, R> TryProcessor<T, R> tryProcessor(
            @Nonnull Function<? super T, ? extends Traverser<? extends R>> mapper
    ) {
        return new TryProcessor<>(mapper);
    }

    /**
     * A helper that simplifies the implementation of {@link AbstractProcessor#tryProcess(int, Object)}
     * for {@code flatMap}-like behavior. The {@code lazyMapper} takes an item
     * and returns a traverser over all output items that should be emitted. The
     * {@link #tryProcess(Object, int)} method obtains and passes the traverser
     * to {@link #emitCooperatively(int, Traverser)}.
     *
     * @param <T> type of the input item
     * @param <R> type of the emitted item
     */
    protected final class TryProcessor<T, R> {
        private Function<? super T, ? extends Traverser<? extends R>> mapper;
        private Traverser<? extends R> outputTraverser;

        TryProcessor(@Nonnull Function<? super T, ? extends Traverser<? extends R>> mapper) {
            this.mapper = mapper;
        }

        public boolean tryProcess(@Nonnull T item, int outputOrdinal) {
            if (outputTraverser == null) {
                outputTraverser = mapper.apply(item);
            }
            if (emitCooperatively(outputOrdinal, outputTraverser)) {
                outputTraverser = null;
                return true;
            }
            return false;
        }

        public boolean tryProcess(@Nonnull T item) {
            return tryProcess(item, -1);
        }
    }

    private void process0(@Nonnull Inbox inbox) {
        for (Object item; (item = inbox.peek()) != null; ) {
            if (!tryProcess0(item)) {
                return;
            }
            inbox.remove();
        }
    }

    private void process1(@Nonnull Inbox inbox) {
        for (Object item; (item = inbox.peek()) != null; ) {
            if (!tryProcess1(item)) {
                return;
            }
            inbox.remove();
        }
    }

    private void process2(@Nonnull Inbox inbox) {
        for (Object item; (item = inbox.peek()) != null; ) {
            if (!tryProcess2(item)) {
                return;
            }
            inbox.remove();
        }
    }

    private void process3(@Nonnull Inbox inbox) {
        for (Object item; (item = inbox.peek()) != null; ) {
            if (!tryProcess3(item)) {
                return;
            }
            inbox.remove();
        }
    }

    private void process4(@Nonnull Inbox inbox) {
        for (Object item; (item = inbox.peek()) != null; ) {
            if (!tryProcess4(item)) {
                return;
            }
            inbox.remove();
        }
    }

    private void processAny(int ordinal, @Nonnull Inbox inbox) {
        for (Object item; (item = inbox.peek()) != null; ) {
            if (!tryProcess(ordinal, item)) {
                return;
            }
            inbox.remove();
        }
    }
}
