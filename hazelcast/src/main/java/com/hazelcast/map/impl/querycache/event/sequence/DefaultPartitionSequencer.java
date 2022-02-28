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

package com.hazelcast.map.impl.querycache.event.sequence;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation of {@link PartitionSequencer}.
 *
 * @see PartitionSequencer
 */
public class DefaultPartitionSequencer implements PartitionSequencer {

    private final AtomicLong sequence;

    public DefaultPartitionSequencer() {
        this.sequence = new AtomicLong(0L);
    }

    @Override
    public long nextSequence() {
        return sequence.incrementAndGet();
    }

    @Override
    public void setSequence(long update) {
        sequence.set(update);
    }

    @Override
    public boolean compareAndSetSequence(long expect, long update) {
        return sequence.compareAndSet(expect, update);
    }

    @Override
    public long getSequence() {
        return sequence.get();
    }

    @Override
    public void reset() {
        sequence.set(0L);
    }
}
