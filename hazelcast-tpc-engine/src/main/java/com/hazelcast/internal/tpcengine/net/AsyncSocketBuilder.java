/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.net;

import com.hazelcast.internal.tpcengine.Option;
import com.hazelcast.internal.tpcengine.Reactor;

/**
 * A {@link AsyncSocket} builder. Can only be used once.
 * <p/>
 * This builder assumes TCP/IPv4. For different types of sockets
 * new configuration options on this builder need to be added or
 * {@link Reactor#newAsyncSocketBuilder()} needs to be modified.
 * <p/>
 * Cast to specific builder for specialized options when available.
 */
public interface AsyncSocketBuilder {

    /**
     * Sets the option on the underlying socket.
     *
     * @param option the option
     * @param value  the value
     * @param <T>    the type of the option/value
     * @return this
     * @throws NullPointerException          when option or value is null.
     * @throws IllegalStateException         when build already has been called
     * @throws UnsupportedOperationException if the option isn't supported.
     * @throws java.io.UncheckedIOException  when something failed while configuring
     *                                       the underlying socket.
     */
    default <T> AsyncSocketBuilder set(Option<T> option, T value) {
        if (setIfSupported(option, value)) {
            return this;
        } else {
            throw new UnsupportedOperationException("'" + option.name() + "' not supported");
        }
    }

    /**
     * Sets the option on the underlying if that option is supported.
     *
     * @param option the option
     * @param value  the value
     * @param <T>    the type of the option/value
     * @return true if the option was supported, false otherwise.
     * @throws NullPointerException          when option or value is null.
     * @throws IllegalStateException         when build already has been called
     * @throws java.io.UncheckedIOException  when something failed while configuring
     *                                       the underlying socket.
     */
    <T> boolean setIfSupported(Option<T> option, T value);

    /**
     * Sets the AsyncSocketReader.
     *
     * @param reader the AsyncSocketReader.
     * @return this
     * @throws NullPointerException  if reader is null.
     * @throws IllegalStateException when build already has been called.
     */
    AsyncSocketBuilder setReader(AsyncSocketReader reader);

    /**
     * Builds the {@link AsyncSocket}.
     *
     * @return the opened AsyncSocket.
     * @throws IllegalStateException when the builder isn't properly configured or when
     *                               build already has been called.
     */
    AsyncSocket build();
}
