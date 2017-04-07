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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.Inbox;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayDeque;

/**
 * Extends {@link ArrayDeque} to implement {@link Inbox}.
 */
// The correctness of this class depends on the fact that the
// implementations of batch draining methods in Inbox delegate
// to poll(). Therefore the class is final.
@SuppressFBWarnings(value = "SE_BAD_FIELD", justification = "Inbox is not intended to be serializable")
public final class ArrayDequeInbox extends ArrayDeque<Object> implements Inbox {

    private final ProgressTracker progTracker;

    /**
     * Constructor to be used just for testing. Uses a private progress
     * tracker.
     */
    public ArrayDequeInbox() {
        this.progTracker = new ProgressTracker();
    }

    /**
     * Constructs the inbox with the provided progress tracker.
     */
    public ArrayDequeInbox(ProgressTracker progTracker) {
        this.progTracker = progTracker;
    }

    @Override
    public Object poll() {
        Object result = super.poll();
        progTracker.madeProgress(result != null);
        return result;
    }

    @Override
    public Object remove() {
        Object result = super.remove();
        progTracker.madeProgress();
        return result;
    }
}
