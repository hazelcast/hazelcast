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

package com.hazelcast.jet.windowing;

import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.impl.util.ProgressTracker;

import java.util.List;

import static org.junit.Assert.assertEquals;

class StreamingTestSupport {
    ArrayDequeInbox inbox = new ArrayDequeInbox();
    ArrayDequeOutbox outbox = new ArrayDequeOutbox(new int[] {1024}, new ProgressTracker());

    static Punctuation punc(long seq) {
        return new Punctuation(seq);
    }

    void assertOutbox(List<?> items) {
        for (Object item : items) {
            assertEquals(item, pollOutbox());
        }
    }

    Object pollOutbox() {
        return outbox.queueWithOrdinal(0).poll();
    }
}
