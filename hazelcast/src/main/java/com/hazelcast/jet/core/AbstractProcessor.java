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

package com.hazelcast.jet.core;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.logging.ILogger;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map.Entry;
import java.util.function.Function;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;


/**
 * Base class to implement custom processors. Simplifies the contract of
 * {@code Processor} with several levels of convenience:
 * <ol><li>
 *     {@link Processor#init(Outbox, Context)} retains the supplied outbox
 *     and the logger retrieved from the context.
 * </li><li>
 *     {@link #process(int, Inbox) process(n, inbox)} delegates to the matching
 *     {@code tryProcessN()} with each item received in the inbox.
 * </li><li>
 *     There is also the general {@link #tryProcess(int, Object)} to which
 *     the {@code tryProcessN} methods delegate by default. It is convenient
 *     to override it when the processor doesn't care which edge an item
 *     originates from. Another convenient idiom is to override {@code
 *     tryProcessN()} for one or two specially treated edges and override
 *     {@link #tryProcess(int, Object)} to process the rest of the edges, which
 *     are treated uniformly.
 * </li><li>
 *     The {@code tryEmit(...)} methods avoid the need to deal with {@code Outbox}
 *     directly.
 * </li><li>
 *     The {@code emitFromTraverser(...)} methods handle the boilerplate of
 *     emission from a traverser. They are especially useful in the
 *     {@link #complete()} step when there is a collection of items to emit.
 *     The {@link Traversers} class contains traversers tailored to simplify
 *     the implementation of {@code complete()}.
 * </li><li>
 *     The {@link FlatMapper FlatMapper} class additionally simplifies the
 *     usage of {@code emitFromTraverser()} inside {@code tryProcess()}, in
 *     a scenario where an input item results in a collection of output
 *     items. {@code FlatMapper} is obtained from one of the factory methods
 *     {@link #flatMapper(Function) flatMapper(...)}.
 * </li></ol>
 *
 * @since Jet 3.0
 */
public abstract class AbstractProcessor implements Processor {

    private ILogger logger;
    private Outbox outbox;

    private Object pendingItem;
    private Entry<?, ?> pendingSnapshotItem;

    // final implementations of the Processor API

    @Override
    public final void init(@Nonnull Outbox outbox, @Nonnull Context context) throws Exception {
        this.outbox = outbox;
        this.logger = context.logger();
        init(context);
    }

    /**
     * Implements the boilerplate of polling the inbox, casting the items to
     * {@code Map.Entry}, and extracting the key and value. Forwards each
     * key-value pair to {@link #restoreFromSnapshot(Object, Object)}.
     */
    @Override
    public final void restoreFromSnapshot(@Nonnull Inbox inbox) {
        for (Entry entry; (entry = (Entry) inbox.poll()) != null; ) {
            restoreFromSnapshot(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Implements the boilerplate of dispatching against the ordinal,
     * taking items from the inbox one by one, and invoking the
     * processing logic on each.
     */
    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void process(int ordinal, @Nonnull Inbox inbox) {
        try {
            switch (ordinal) {
                case 0:
                    process0(inbox);
                    break;
                case 1:
                    process1(inbox);
                    break;
                case 2:
                    process2(inbox);
                    break;
                case 3:
                    process3(inbox);
                    break;
                case 4:
                    process4(inbox);
                    break;
                default:
                    processAny(ordinal, inbox);
            }
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }


    //                Callback methods designed to be overridden by subclasses


    /**
     * Method that can be overridden to perform any necessary initialization
     * for the processor. It is called exactly once and strictly before any of
     * the processing methods ({@link #process(int, Inbox) process()} and
     * {@link #complete()}), but after the outbox and {@link #getLogger()
     * logger} have been initialized.
     * <p>
     * Subclasses are not required to call this superclass method, it does
     * nothing.
     *
     * @param context the {@link Processor.Context context} associated with this
     *                processor
     */
    protected void init(@Nonnull Context context) throws Exception {
    }

    /**
     * Tries to process the supplied input item, which was received from the
     * edge with the supplied ordinal. May choose to process only partially
     * and return {@code false}, in which case it will be called again later
     * with the same {@code (ordinal, item)} combination before any other
     * processing method is called.
     * <p>
     * The default implementation throws an {@code UnsupportedOperationException}.
     * <p>
     * <strong>NOTE:</strong> unless the processor doesn't differentiate between
     * its inbound edges, the first choice should be leaving this method alone
     * and instead overriding the specific {@code tryProcessN()} methods for
     * each ordinal the processor expects.
     *
     * @param ordinal ordinal of the edge that delivered the item
     * @param item    item to be processed
     * @return {@code true} if this item has now been processed,
     *         {@code false} otherwise.
     */
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        throw new UnsupportedOperationException("Missing implementation in " + getClass());
    }

    /**
     * Tries to process the supplied input item, which was received from the
     * edge with ordinal 0. May choose to process only partially and return
     * {@code false}, in which case it will be called again later with the same
     * item before any other processing method is called.
     * <p>
     * The default implementation delegates to {@link #tryProcess(int, Object)
     * tryProcess(0, item)}.
     *
     * @param item    item to be processed
     * @return {@code true} if this item has now been processed,
     *         {@code false} otherwise.
     */
    protected boolean tryProcess0(@Nonnull Object item) throws Exception {
        return tryProcess(0, item);
    }

    /**
     * Tries to process the supplied input item, which was received from the
     * edge with ordinal 1. May choose to process only partially and return
     * {@code false}, in which case it will be called again later with the same
     * item before any other processing method is called.
     * <p>
     * The default implementation delegates to {@link #tryProcess(int, Object)
     * tryProcess(1, item)}.
     *
     * @param item    item to be processed
     * @return {@code true} if this item has now been processed,
     *         {@code false} otherwise.
     */
    protected boolean tryProcess1(@Nonnull Object item) throws Exception {
        return tryProcess(1, item);
    }

    /**
     * Tries to process the supplied input item, which was received from the
     * edge with ordinal 2. May choose to process only partially and return
     * {@code false}, in which case it will be called again later with the same
     * item before any other processing method is called.
     * <p>
     * The default implementation delegates to {@link #tryProcess(int, Object)
     * tryProcess(2, item)}.
     *
     * @param item    item to be processed
     * @return {@code true} if this item has now been processed,
     *         {@code false} otherwise.
     */
    protected boolean tryProcess2(@Nonnull Object item) throws Exception {
        return tryProcess(2, item);
    }

    /**
     * Tries to process the supplied input item, which was received from the
     * edge with ordinal 3. May choose to process only partially and return
     * {@code false}, in which case it will be called again later with the same
     * item before any other processing method is called.
     * <p>
     * The default implementation delegates to {@link #tryProcess(int, Object)
     * tryProcess(3, item)}.
     *
     * @param item    item to be processed
     * @return {@code true} if this item has now been processed,
     *         {@code false} otherwise.
     */
    protected boolean tryProcess3(@Nonnull Object item) throws Exception {
        return tryProcess(3, item);
    }

    /**
     * Tries to process the supplied input item, which was received from the
     * edge with ordinal 4. May choose to process only partially and return
     * {@code false}, in which case it will be called again later with the same
     * item before any other processing method is called.
     * <p>
     * The default implementation delegates to {@link #tryProcess(int, Object)
     * tryProcess(4, item)}.
     *
     * @param item    item to be processed
     * @return {@code true} if this item has now been processed,
     *         {@code false} otherwise.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    protected boolean tryProcess4(@Nonnull Object item) throws Exception {
        return tryProcess(4, item);
    }

    /**
     * Called to restore one key-value pair from the snapshot to processor's
     * internal state.
     * <p>
     * The default implementation throws an {@code
     * UnsupportedOperationException}, but it will not be called unless you
     * override {@link #saveToSnapshot()}.
     *
     * @param key      key of the entry from the snapshot
     * @param value    value of the entry from the snapshot
     */
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        throw new UnsupportedOperationException("Missing implementation in " + getClass());
    }

    /**
     * This basic implementation only forwards the passed watermark.
     */
    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return tryEmit(watermark);
    }


    //                  Convenience methods for subclasses, non-overridable


    /**
     * Returns the logger associated with this processor instance.
     */
    protected final ILogger getLogger() {
        return logger;
    }

    protected final Outbox getOutbox() {
        return outbox;
    }

    /**
     * Offers the item to the outbox bucket at the supplied ordinal.
     * <p>
     * Emitted items should not be subsequently mutated because the same
     * instance might be used by a downstream processor in a different thread,
     * causing concurrent access.
     *
     * @return {@code true}, if the item was accepted. If {@code false} is
     * returned, the call must be retried later with the same (or equal) item.
     */
    @CheckReturnValue
    protected final boolean tryEmit(int ordinal, @Nonnull Object item) {
        return outbox.offer(ordinal, item);
    }

    /**
     * Offers the item to all the outbox buckets (except the snapshot outbox).
     * <p>
     * Emitted items should not be subsequently mutated because the same
     * instance might be used by a downstream processor in a different thread,
     * causing concurrent access.
     *
     * @return {@code true}, if the item was accepted. If {@code false} is
     * returned, the call must be retried later with the same (or equal) item.
     */
    @CheckReturnValue
    protected final boolean tryEmit(@Nonnull Object item) {
        return outbox.offer(item);
    }

    /**
     * Offers the item to the outbox buckets identified in the supplied array.
     * <p>
     * Emitted items should not be subsequently mutated because the same
     * instance might be used by a downstream processor in a different thread,
     * causing concurrent access.
     *
     * @return {@code true}, if the item was accepted. If {@code false} is
     * returned, the call must be retried later with the same (or equal) item.
     */
    @CheckReturnValue
    protected final boolean tryEmit(@Nonnull int[] ordinals, @Nonnull Object item) {
        return outbox.offer(ordinals, item);
    }

    /**
     * Obtains items from the traverser and offers them to the outbox's buckets
     * identified in the supplied array. If the outbox refuses an item, it backs
     * off and returns {@code false}.
     * <p>
     * Emitted items should not be subsequently mutated because the same
     * instance might be used by a downstream processor in a different thread,
     * causing concurrent access.
     * <p>
     * If this method returns {@code false}, then the caller must retain the
     * traverser and pass it again in the subsequent invocation of this method,
     * so as to resume emitting where it left off.
     * <p>
     * For simplified usage from {@link #tryProcess(int, Object)
     * tryProcess(ordinal, item)} methods, see {@link FlatMapper}.
     *
     * @param ordinals ordinals of the target bucket
     * @param traverser traverser over items to emit
     * @return whether the traverser has been exhausted
     */
    @SuppressWarnings("unchecked")
    protected final <E> boolean emitFromTraverser(@Nonnull int[] ordinals, @Nonnull Traverser<E> traverser) {
        E item;
        if (pendingItem != null) {
            item = (E) pendingItem;
            pendingItem = null;
        } else {
            item = traverser.next();
        }
        for (; item != null; item = traverser.next()) {
            if (!tryEmit(ordinals, item)) {
                pendingItem = item;
                return false;
            }
        }
        return true;
    }

    /**
     * Obtains items from the traverser and offers them to the outbox's buckets
     * identified in the supplied array. If the outbox refuses an item, it backs
     * off and returns {@code false}.
     * <p>
     * Do not mutate the items you emit because the downstream processor may be
     * using them in a different thread, resulting in concurrent access.
     * <p>
     * If this method returns {@code false}, then you must retain the traverser
     * and pass it again in the subsequent invocation of this method, so as to
     * resume emitting where you left off.
     * <p>
     * For simplified usage in {@link #tryProcess(int, Object)
     * tryProcess(ordinal, item)} methods, see {@link FlatMapper}.
     *
     * @param ordinal ordinal of the target bucket
     * @param traverser traverser over items to emit
     * @return whether the traverser has been exhausted
     */
    protected final <E> boolean emitFromTraverser(int ordinal, @Nonnull Traverser<E> traverser) {
        E item;
        if (pendingItem != null) {
            item = (E) pendingItem;
            pendingItem = null;
        } else {
            item = traverser.next();
        }
        for (; item != null; item = traverser.next()) {
            if (!tryEmit(ordinal, item)) {
                pendingItem = item;
                return false;
            }
        }
        return true;
    }

    /**
     * Convenience for {@link #emitFromTraverser(int, Traverser)}
     * which emits to all ordinals.
     */
    protected final boolean emitFromTraverser(@Nonnull Traverser<?> traverser) {
        return emitFromTraverser(-1, traverser);
    }

    /**
     * Offers one key-value pair to the snapshot bucket.
     * <p>
     * The type of the offered key determines which processors receive the key
     * and value pair when it is restored. If the key is of type {@link
     * BroadcastKey}, the entry will be restored to all processor instances.
     * Otherwise, the key will be distributed according to default partitioning
     * and only a single processor instance will receive the key.
     * <p>
     * Keys and values offered to snapshot are serialized and can be further
     * mutated as soon as this method returns.
     *
     * @return {@code true}, if the item was accepted. If {@code false} is
     * returned, the call must be retried later with the same (or equal) key
     * and value.
     */
    @CheckReturnValue
    protected final boolean tryEmitToSnapshot(@Nonnull Object key, @Nonnull Object value) {
        return outbox.offerToSnapshot(key, value);
    }

    /**
     * Obtains items from the traverser and offers them to the snapshot bucket
     * of the outbox. Each item is a {@code Map.Entry} and its key and value
     * are passed as the two arguments of {@link #tryEmitToSnapshot(Object, Object)}.
     * If the outbox refuses an item, it backs off and returns {@code false}.
     * <p>
     * Keys and values offered to snapshot are serialized and can be further
     * mutated as soon as this method returns.
     * <p>
     * If this method returns {@code false}, then the caller must retain the
     * traverser and pass it again in the subsequent invocation of this method,
     * so as to resume emitting where it left off.
     * <p>
     * The type of the offered key determines which processors receive the key
     * and value pair when it is restored. If the key is of type {@link
     * BroadcastKey}, the entry will be restored to all processor instances.
     * Otherwise, the key will be distributed according to default partitioning
     * and only a single processor instance will receive the key.
     *
     * @param traverser traverser over the items to emit to the snapshot
     * @return whether the traverser has been exhausted
     */
    protected final <T extends Entry<?, ?>> boolean emitFromTraverserToSnapshot(@Nonnull Traverser<T> traverser) {
        Entry<?, ?> item;
        if (pendingSnapshotItem != null) {
            item = pendingSnapshotItem;
            pendingSnapshotItem = null;
        } else {
            item = traverser.next();
        }
        for (; item != null; item = traverser.next()) {
            if (!tryEmitToSnapshot(item.getKey(), item.getValue())) {
                pendingSnapshotItem = item;
                return false;
            }
        }
        return true;
    }

    /**
     * Factory of {@link FlatMapper}. The {@code FlatMapper} will emit items to
     * the given output ordinal.
     */
    @Nonnull
    protected final <T, R> FlatMapper<T, R> flatMapper(
            int ordinal, @Nonnull Function<? super T, ? extends Traverser<? extends R>> mapper
    ) {
        return ordinal != -1 ? flatMapper(new int[] {ordinal}, mapper) : flatMapper(mapper);
    }

    /**
     * Factory of {@link FlatMapper}. The {@code FlatMapper} will emit items to
     * all defined output ordinals.
     */
    @Nonnull
    protected final <T, R> FlatMapper<T, R> flatMapper(
            @Nonnull Function<? super T, ? extends Traverser<? extends R>> mapper
    ) {
        return new FlatMapper<>(null, mapper);
    }

    /**
     * Factory of {@link FlatMapper}. The {@code FlatMapper} will emit items to
     * the ordinals identified in the array.
     */
    @Nonnull
    protected final <T, R> FlatMapper<T, R> flatMapper(
            @Nonnull int[] ordinals, @Nonnull Function<? super T, ? extends Traverser<? extends R>> mapper
    ) {
        return new FlatMapper<>(ordinals, mapper);
    }


    //               End of convenience methods for subclass, non-overridable


    /**
     * A helper that simplifies the implementation of {@link #tryProcess(int,
     * Object) tryProcess(ordinal, item)} for emitting collections. User
     * supplies a {@code mapper} which takes an item and returns a traverser
     * over all output items that should be emitted. The {@link
     * #tryProcess(Object)} method obtains and passes the traverser to {@link
     * #emitFromTraverser(int, Traverser)}.
     *
     * Example:
     * <pre>
     * public static class SplitWordsP extends AbstractProcessor {
     *
     *    {@code private FlatMapper<String, String> flatMapper =
     *             flatMapper(item -> Traverser.over(item.split("\\W")));}
     *
     *    {@code @Override}
     *     protected boolean tryProcess(int ordinal, Object item) throws Exception {
     *         return flatMapper.tryProcess((String) item);
     *     }
     * }</pre>
     *
     * @param <T> type of the input item
     * @param <R> type of the emitted item
     */
    protected final class FlatMapper<T, R> {
        private final int[] outputOrdinals;
        private final Function<? super T, ? extends Traverser<? extends R>> mapper;
        private Traverser<? extends R> outputTraverser;

        private FlatMapper(@Nullable int[] outputOrdinals,
                           @Nonnull Function<? super T, ? extends Traverser<? extends R>> mapper) {
            this.outputOrdinals = outputOrdinals;
            this.mapper = mapper;
        }

        /**
         * Method designed to be called from one of {@code AbstractProcessor#tryProcessX()}
         * methods. The calling method must return this method's return
         * value.
         *
         * @param item the item to process
         * @return what the calling {@code tryProcessX()} method should return
         */
        public boolean tryProcess(@Nonnull T item) {
            if (outputTraverser == null) {
                outputTraverser = mapper.apply(item);
            }
            if (emit()) {
                outputTraverser = null;
                return true;
            }
            return false;
        }

        private boolean emit() {
            return outputOrdinals != null
                    ? emitFromTraverser(outputOrdinals, outputTraverser)
                    : emitFromTraverser(outputTraverser);
        }
    }


    // The processN methods contain repeated looping code in order to give an
    // easier job to the JIT compiler to optimize each case independently, and
    // to ensure that ordinal is dispatched on just once per process(ordinal,
    // inbox) call.


    private void process0(@Nonnull Inbox inbox) throws Exception {
        for (Object item; (item = inbox.peek()) != null && tryProcess0(item); ) {
            inbox.remove();
        }
    }

    private void process1(@Nonnull Inbox inbox) throws Exception {
        for (Object item; (item = inbox.peek()) != null && tryProcess1(item); ) {
            inbox.remove();
        }
    }

    private void process2(@Nonnull Inbox inbox) throws Exception {
        for (Object item; (item = inbox.peek()) != null && tryProcess2(item); ) {
            inbox.remove();
        }
    }

    private void process3(@Nonnull Inbox inbox) throws Exception {
        for (Object item; (item = inbox.peek()) != null && tryProcess3(item); ) {
            inbox.remove();
        }
    }

    private void process4(@Nonnull Inbox inbox) throws Exception {
        for (Object item; (item = inbox.peek()) != null && tryProcess4(item); ) {
            inbox.remove();
        }
    }

    private void processAny(int ordinal, @Nonnull Inbox inbox) throws Exception {
        for (Object item; (item = inbox.peek()) != null && tryProcess(ordinal, item); ) {
            inbox.remove();
        }
    }
}
