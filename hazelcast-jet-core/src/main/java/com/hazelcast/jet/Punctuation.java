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

import java.io.Serializable;

/**
 * A stream item that carries a sequence value denoting the barrier between
 * "on time" and "late" items.
 * <pre>{@code
 *   boolean itemIsLate = itemSeq < punc.seq();
 *   boolean itemIsOnTime = itemSeq >= punc.seq();
 * }</pre>
 */
public final class Punctuation implements Serializable {

    private final long seq;

    /**
     * Constructs a new punctuation item.
     */
    public Punctuation(long seq) {
        this.seq = seq;
    }

    /**
     * Returns the sequence number of this punctuation.
     */
    public long seq() {
        return seq;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof Punctuation && this.seq == ((Punctuation) o).seq;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(seq);
    }

    @Override
    public String toString() {
        return "Punctuation{seq=" + seq + '}';
    }
}
