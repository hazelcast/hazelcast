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

package com.hazelcast.jet.impl;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.DoneItem.DONE_ITEM;

public class MockOutboundStream extends OutboundEdgeStream {

    public MockOutboundStream(int ordinal, int capacity) {
        super(ordinal, 1024, new MockOutboundCollector(capacity));
    }

    public List<Object> getBuffer() {
        return ((MockOutboundCollector) getCollector()).getBuffer();
    }

    private static class MockOutboundCollector implements OutboundCollector {

        private final ArrayList<Object> buffer;
        private final int capacity;

        public MockOutboundCollector(int capacity) {
            this.capacity = capacity;
            this.buffer = new ArrayList<>(capacity);
        }


        @Override
        public ProgressState offer(Object item) {
            if (buffer.size() == capacity) {
                return ProgressState.NO_PROGRESS;
            }
            buffer.add(item);
            return ProgressState.DONE;
        }

        @Override
        public ProgressState close() {
            buffer.add(DONE_ITEM);
            return ProgressState.DONE;
        }

        @Override
        public int[] getPartitions() {
            return null;
        }

        public List<Object> drain() {
            List<Object> copy = new ArrayList<>(this.buffer);
            this.buffer.clear();
            return copy;
        }

        public List<Object> getBuffer() {
            return buffer;
        }

    }
}


