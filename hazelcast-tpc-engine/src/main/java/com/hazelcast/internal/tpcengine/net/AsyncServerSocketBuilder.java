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

import java.util.function.Consumer;

/**
 * A builder for {@link AsyncServerSocket} instances. Can only be used once.
 * <p/>
 * This builder assumes TCP/IPv4. For different types of sockets
 * new configuration options on this builder need to be added.
 * <p/>
 * Cast to specific builder for specialized options when available.
 */
public interface AsyncServerSocketBuilder {

    /**
     * Sets the option on the underlying socket.
     *
     * @param option the option
     * @param value  the value
     * @param <T>    the type of the option/value
     * @return this
     * @throws NullPointerException          when option or value is <code>null</code>.
     * @throws IllegalStateException         when build already has been called
     * @throws UnsupportedOperationException if the option isn't supported.
     * @throws java.io.UncheckedIOException  when something failed while configuring the underlying socket.
     */
    default <T> AsyncServerSocketBuilder set(Option<T> option, T value) {
        if (setIfSupported(option, value)) {
            return this;
        } else {
            throw new UnsupportedOperationException("'" + option.name() + "' not supported");
        }
    }

    /**
     * Sets the option on the underlying socket if that option is supported.
     *
     * @param option the option
     * @param value  the value
     * @param <T>    the type of the option/value
     * @return true if the option was supported, false otherwise.
     * @throws NullPointerException         when option or value is <code>null</code>.
     * @throws IllegalStateException        when build already has been called
     * @throws java.io.UncheckedIOException when something failed while configuring the underlying socket.
     */
    <T> boolean setIfSupported(Option<T> option, T value);

    /**
     * Sets the consumer for accept requests.
     *
     * @param consumer the consumer
     * @return this
     * @throws NullPointerException  if consumer is <code>null</code>.
     * @throws IllegalStateException when build already has been called.
     */
    AsyncServerSocketBuilder setAcceptConsumer(Consumer<AcceptRequest> consumer);

    /**
     * Builds the AsyncServerSocket.
     *
     * @return the build AsyncServerSocket.
     * @throws IllegalStateException        when the build already has been called or when the builder
     *                                      has not been properly configured.
     * @throws java.io.UncheckedIOException when the socket could not be build.
     */
    AsyncServerSocket build();
}
