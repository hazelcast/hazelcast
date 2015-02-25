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

package com.hazelcast.nio.tcp;

import com.hazelcast.logging.ILogger;

import java.nio.channels.SelectionKey;

public final class OutSelectorImpl extends AbstractIOSelector {

    // This field will be incremented by a single thread --> the OutSelectorImpl. It can be read by multiple threads.
    private volatile long writeEvents;

    public OutSelectorImpl(ThreadGroup threadGroup, String tname, ILogger logger, IOSelectorOutOfMemoryHandler oomeHandler) {
        super(threadGroup, tname, logger, oomeHandler);
    }

    /**
     * Returns the current number of write events that have been processed by this OutSelectorImpl.
     *
     * This method is thread-safe.
     *
     * @return the number of write events.
     */
    public long getWriteEvents() {
        return writeEvents;
    }

    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"VO_VOLATILE_INCREMENT" })
    protected void handleSelectionKey(SelectionKey sk) {
        if (sk.isValid() && sk.isWritable()) {
            writeEvents++;
            SelectionHandler handler = (SelectionHandler) sk.attachment();
            handler.handle();
        }
    }
}
