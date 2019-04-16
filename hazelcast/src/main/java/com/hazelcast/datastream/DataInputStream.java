/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream;

/**
 * A Subscriber for the {@link DataStream}.
 * <p>
 * Implementations are thread-safe.
 *
 * @param <E>
 */
public interface DataInputStream<E> {

    /**
     * Reads a single message from the stream. Will block until a message is received,
     * or when the calling thread is interrupted.
     *
     * @return
     * @throws InterruptedException
     */
    E read() throws InterruptedException;

    /**
     * @param timeoutMs
     * @return
     * @throws InterruptedException
     */
    E read(long timeoutMs) throws InterruptedException;

    /**
     * Polls the DataInputStream if there is a message. If a message is available, it is returned,
     * but if none is available, null is returned.
     *
     * @return the message or null.
     */
    E poll();
}
