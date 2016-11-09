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

package com.hazelcast.jet2.impl;

import com.hazelcast.jet2.Inbox;
import com.hazelcast.jet2.Outbox;
import com.hazelcast.jet2.Processor;

import javax.annotation.Nonnull;

public abstract class AbstractProcessor implements Processor {

    private Outbox outbox;

    @Override
    public void init(@Nonnull Outbox outbox) {
        this.outbox = outbox;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation delegates to the item-at-a-time {@link #process(int, Object)}.
     */
    @Override
    public void process(int ordinal, Inbox inbox) {
        for (Object item; (item = inbox.peek()) != null; ) {
            if (!process(ordinal, item)) {
                return;
            }
            inbox.remove();
        }
    }

    /**
     * Processes the supplied input item.
     *
     * @param ordinal ordinal of the input where the item originates from
     * @param item    item to be processed
     * @return <code>true</code> if this item has now been processed, <code>false</code> otherwise.
     */
    protected boolean process(int ordinal, Object item) {
        throw new UnsupportedOperationException("");
    }

    protected Outbox getOutbox() {
        return outbox;
    }

    protected void emit(int ordinal, Object item) {
        outbox.add(ordinal, item);
    }

    protected void emit(Object item) {
        outbox.add(item);
    }
}
