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

package com.hazelcast.internal.networking.spinning;

import com.hazelcast.util.ThreadUtil;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.lang.System.arraycopy;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

public class SpinningInputThread extends Thread {

    private static final ChannelReaders SHUTDOWN = new ChannelReaders();
    private static final AtomicReferenceFieldUpdater<SpinningInputThread, ChannelReaders> CONNECTION_HANDLERS
            = newUpdater(SpinningInputThread.class, ChannelReaders.class, "channelReaders");

    private volatile ChannelReaders channelReaders = new ChannelReaders();

    SpinningInputThread(String hzName) {
        super(ThreadUtil.createThreadName(hzName, "in-thread"));
    }

    void register(SpinningChannelReader reader) {
        for (; ; ) {
            ChannelReaders current = channelReaders;
            if (current == SHUTDOWN) {
                return;
            }

            int length = current.readers.length;
            SpinningChannelReader[] newReaders = new SpinningChannelReader[length + 1];
            arraycopy(current.readers, 0, newReaders, 0, length);
            newReaders[length] = reader;

            ChannelReaders update = new ChannelReaders(newReaders);
            if (CONNECTION_HANDLERS.compareAndSet(this, current, update)) {
                return;
            }
        }
    }

    void unregister(SpinningChannelReader reader) {
        for (; ; ) {
            ChannelReaders current = channelReaders;
            if (current == SHUTDOWN) {
                return;
            }

            int indexOf = current.indexOf(reader);
            if (indexOf == -1) {
                return;
            }

            int length = current.readers.length;
            SpinningChannelReader[] newReaders = new SpinningChannelReader[length - 1];

            int destIndex = 0;
            for (int sourceIndex = 0; sourceIndex < length; sourceIndex++) {
                if (sourceIndex != indexOf) {
                    newReaders[destIndex] = current.readers[sourceIndex];
                    destIndex++;
                }
            }

            ChannelReaders update = new ChannelReaders(newReaders);
            if (CONNECTION_HANDLERS.compareAndSet(this, current, update)) {
                return;
            }
        }
    }

    public void shutdown() {
        channelReaders = SHUTDOWN;
        interrupt();
    }

    @Override
    public void run() {
        for (; ; ) {
            ChannelReaders readers = channelReaders;

            if (readers == SHUTDOWN) {
                return;
            }

            for (SpinningChannelReader reader : readers.readers) {
                try {
                    reader.read();
                } catch (Throwable t) {
                    reader.onFailure(t);
                }
            }
        }
    }

    static class ChannelReaders {
        final SpinningChannelReader[] readers;

        ChannelReaders() {
            this(new SpinningChannelReader[0]);
        }

        ChannelReaders(SpinningChannelReader[] readers) {
            this.readers = readers;
        }

        public int indexOf(SpinningChannelReader reader) {
            for (int k = 0; k < readers.length; k++) {
                if (readers[k] == reader) {
                    return k;
                }
            }

            return -1;
        }
    }
}
