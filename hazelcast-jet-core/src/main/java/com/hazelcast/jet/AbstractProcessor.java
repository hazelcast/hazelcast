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
     * Delegates directly to {@link Outbox#add(int, Object)}.
     */
    protected void emit(int ordinal, Object item) {
        outbox.add(ordinal, item);
    }

    /**
     * Delegates directly to {@link Outbox#add(Object)}.
     */
    protected void emit(Object item) {
        outbox.add(item);
    }
}
