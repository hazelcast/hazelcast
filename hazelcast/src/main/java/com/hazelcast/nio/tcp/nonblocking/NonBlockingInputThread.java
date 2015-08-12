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

package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.internal.metrics.CompositeProbe;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeName;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.counters.SwCounter;

import java.nio.channels.SelectionKey;

import static com.hazelcast.util.counters.SwCounter.newSwCounter;

public final class NonBlockingInputThread extends NonBlockingIOThread  {

    // This field will be incremented by this thread. It can be read by multiple threads.
    @Probe
    private final SwCounter readEvents = newSwCounter();

    public NonBlockingInputThread(ThreadGroup threadGroup, String threadName, ILogger logger,
                                  NonBlockingIOThreadOutOfMemoryHandler oomeHandler) {
        super(threadGroup, threadName, logger, oomeHandler);
    }

    /**
     * Returns the current number of read events that have been processed by this NonBlockingInputThread.
     *
     * This method is thread-safe.
     *
     * @return the number of read events.
     */
    public long getReadEvents() {
        return readEvents.get();
    }

    @Override
    protected void handleSelectionKey(SelectionKey sk) {
        if (sk.isValid() && sk.isReadable()) {
            readEvents.inc();
            SelectionHandler handler = (SelectionHandler) sk.attachment();
            handler.handle();
        }
    }
}
