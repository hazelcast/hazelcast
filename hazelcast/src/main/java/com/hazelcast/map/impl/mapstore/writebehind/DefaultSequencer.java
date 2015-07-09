/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation of a {@link Sequencer}.
 * Sharing an instance of this implementation between threads is safe.
 *
 * @see Sequencer
 */
public class DefaultSequencer implements Sequencer {

    private final AtomicLong head = new AtomicLong();
    private final AtomicLong tail = new AtomicLong();

    public DefaultSequencer() {
        init();
    }


    @Override
    public long headSequence() {
        return head.get();
    }

    @Override
    public long incrementTail() {
        return tail.incrementAndGet();
    }

    @Override
    public void setHeadSequence(long sequence) {
        this.head.set(sequence);
    }

    @Override
    public long tailSequence() {
        return tail.get();
    }

    @Override
    public long incrementHead() {
        return head.incrementAndGet();
    }

    @Override
    public void setTailSequence(long sequence) {
        this.tail.set(sequence);
    }

    @Override
    public void init() {
        head.set(0L);
        tail.set(0L);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(headSequence());
        out.writeLong(tailSequence());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        long headValue = in.readLong();
        long tailValue = in.readLong();

        head.set(headValue);
        tail.set(tailValue);
    }

    @Override
    public String toString() {
        return "DefaultSequencer{"
                + "head=" + head
                + ", tail=" + tail
                + '}';
    }
}

