/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import javax.annotation.Nonnull;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Base class to implement custom processors. Simplifies the contract of
 * {@code Processor} by abstracting away {@code Inbox} and {@code Outbox},
 * instead requiring a simple {@link #tryProcess(int, Object)} callback
 * to be implemented. The implementation should use the {@code emit()}
 * methods to emit its output. There are two overloads of {@code emit()} and
 * each delegates to the overload of {@code Outbox.add()} with the same
 * signature.
 * <p>
 * Processors which cannot be implemented in terms of the simplified API can
 * directly override Processor's {@code process()} method while still being
 * spared of the {@code init()}-related boilerplate.
 */
public abstract class AbstractProcessor implements Processor {

    private Outbox outbox;

    @Override
    public void init(@Nonnull Outbox outbox) {
        this.outbox = outbox;
    }

    /**
     * Implements the boilerplate of taking items from the
     * inbox and processing them one by one.
     */
    @Override
    public void process(int ordinal, Inbox inbox) {
        for (Object item; (item = inbox.peek()) != null; ) {
            if (!tryProcess(ordinal, item)) {
                return;
            }
            inbox.remove();
        }
    }

    /**
     * Tries to process the supplied input item. May choose to process
     * only partially and return {@code false}, in which case it will be
     * called again later with the same {@code (ordinal, item)} combination.
     *
     * @param ordinal ordinal of the input which the item originates from
     * @param item    item to be processed
     * @return {@code true} if this item has now been processed,
     *         {@code false} otherwise.
     */
    protected boolean tryProcess(int ordinal, Object item) {
        throw new UnsupportedOperationException("Missing implementation");
    }

    /**
     * @return the outbox received in the {@code init()} method call.
     */
    protected Outbox getOutbox() {
        return outbox;
    }

    /**
     * Emits the item to the outbox bucket at the supplied ordinal.
     */
    protected void emit(int ordinal, Object item) {
        outbox.add(ordinal, item);
    }

    /**
     * Emits the item to all the outbox buckets.
     */
    protected void emit(Object item) {
        outbox.add(item);
    }

    /**
     * Emits the items obtained from the supplier to the outbox bucket with the supplied ordinal,
     * in a cooperative fashion: if the outbox reports a high-water condition, backs off and
     * returns {@code false}.
     * <p>
     * The item supplier is expected to behave as follows:
     * <ol><li>
     *     Each invocation of its {@code get(()} method must return the next item to emit.
     * </li><li>
     *     If this method returns {@code false}, then the same supplier must be retained by
     *     the caller and used again in the subsequent invocation of this method, so as to
     *     resume emitting where it left off.
     * </li><li>
     *     When all the items have been supplied, {@code get()} must return {@code null}
     *     in all subsequent invocations.
     * </li></ol>
     *
     * @param ordinal ordinal of the target bucket
     * @param itemSupplier supplier of all items. It will be called repeatedly until
     *                     it returns {@code null} or the outbox bucket reaches high water.
     * @return {@code true} if all the items were emitted ({@code itemSupplier}
     *         returned {@code null})
     */
    protected boolean emitCooperatively(int ordinal, Supplier<?> itemSupplier) {
        while (true) {
            if (getOutbox().isHighWater(ordinal)) {
                return false;
            }
            final Object item = itemSupplier.get();
            if (item == null) {
                return true;
            }
            emit(ordinal, item);
        }
    }

    protected boolean emitCooperatively(Supplier<?> itemSupplier) {
        return emitCooperatively(-1, itemSupplier);
    }

    protected <T, R> TryProcessor<T, R> tryProcessor(Function<T, Supplier<R>> lazyTransformer) {
        return new TryProcessor<>(lazyTransformer);
    }

    protected class TryProcessor<T, R> {

        private final Function<T, Supplier<R>> lazyTransformer;
        private Supplier<R> outputSupplier;

        private TryProcessor(Function<T, Supplier<R>> lazyTransformer) {
            this.lazyTransformer = lazyTransformer;
        }

        public boolean tryProcess(int ordinal, T item) {
            if (outputSupplier == null) {
                outputSupplier = lazyTransformer.apply(item);
            }
            if (emitCooperatively(ordinal, outputSupplier)) {
                outputSupplier = null;
                return true;
            }
            return false;
        }
    }
}
