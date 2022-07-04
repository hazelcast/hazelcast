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

import com.hazelcast.jet.core.Outbox;

public interface OutboxInternal extends Outbox {

    /**
     * Resets the counter that prevents adding more than {@code batchSize}
     * items until this method is called again. Note that the counter may
     * jump to the "full" state even before the outbox accepted that many items.
     */
    void reset();

    /**
     * Blocks the outbox so it allows the caller only to offer the current
     * unfinished item. If there is no unfinished item, the outbox will reject
     * all items until you call {@link #unblock()}.
     */
    void block();

    /**
     * Removes the effect of a previous {@link #block()} call (if any).
     */
    void unblock();

    /**
     * Returns the timestamp of the last forwarded watermark.
     * <p>
     * If there was no watermark added, it returns {@code Long.MIN_VALUE}. Can
     * be called from a concurrent thread.
     */
    long lastForwardedWm();

}
