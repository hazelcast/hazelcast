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

package com.hazelcast.ringbuffer;

import com.hazelcast.spi.exception.SilentException;
import com.hazelcast.spi.impl.operationservice.WrappableException;

/**
 * An {@link RuntimeException} that is thrown when accessing an item in the {@link Ringbuffer} using a sequence that is smaller
 * than the current head sequence and that the ringbuffer store is disabled. This means that the item isn't available in the
 * ringbuffer and it cannot be loaded from the store either, thus being completely unavailable.
 */
public class StaleSequenceException extends RuntimeException implements SilentException,
        WrappableException<StaleSequenceException> {

    private final long headSeq;

    /**
     * Creates a StaleSequenceException with the given message.
     *
     * @param message the message
     * @param headSeq the last known head sequence.
     */
    public StaleSequenceException(String message, long headSeq) {
        super(message);
        this.headSeq = headSeq;
    }

    public StaleSequenceException(String message, long headSeq, Throwable cause) {
        super(message, cause);
        this.headSeq = headSeq;
    }

    /**
     * Returns the last known head sequence. Beware that this sequence could already be stale again if you want to use it
     * to do a {@link com.hazelcast.ringbuffer.Ringbuffer#readOne(long)}.
     *
     * @return last known head sequence.
     */
    public long getHeadSeq() {
        return headSeq;
    }

    @Override
    public StaleSequenceException wrap() {
        return new StaleSequenceException(getMessage(), headSeq, this);
    }
}
