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

package com.hazelcast.spi.impl.sequence;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public final class CallIdSequenceWithoutBackpressure implements CallIdSequence {

    private static final AtomicLongFieldUpdater<CallIdSequenceWithoutBackpressure> HEAD
            = AtomicLongFieldUpdater.newUpdater(CallIdSequenceWithoutBackpressure.class, "head");

    private volatile long head;

    @Override
    public long getLastCallId() {
        return head;
    }

    @Override
    public int getMaxConcurrentInvocations() {
        return Integer.MAX_VALUE;
    }

    @Override
    public long next() {
        return forceNext();
    }

    @Override
    public long forceNext() {
        return HEAD.incrementAndGet(this);
    }

    @Override
    public void complete() {
        //no-op
    }
}
