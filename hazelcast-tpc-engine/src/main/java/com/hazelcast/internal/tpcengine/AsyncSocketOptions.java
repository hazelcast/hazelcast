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

package com.hazelcast.internal.tpcengine;

/**
 * Options for the {@link AsyncSocket} and {@link AsyncServerSocket}.
 * <p>
 * Reason for the name: there already exists a class com.hazelcast.client.config.SocketOptions
 * and java.net.SocketOptions.
 */
public interface AsyncSocketOptions {
    /**
     * See {@link java.net.SocketOptions#SO_RCVBUF}.
     */
    Option<Integer> SO_RCVBUF = new Option<>("SO_RCVBUF", Integer.class);

    /**
     * See {@link java.net.SocketOptions#SO_SNDBUF}
     */
    Option<Integer> SO_SNDBUF = new Option<>("SO_SNDBUF", Integer.class);

    /**
     * See {@link java.net.SocketOptions#SO_KEEPALIVE}
     */
    Option<Boolean> SO_KEEPALIVE = new Option<>("SO_KEEPALIVE", Boolean.class);

    /**
     * See {@link java.net.SocketOptions#SO_REUSEPORT}
     */
    Option<Boolean> SO_REUSEPORT = new Option<>("SO_REUSEPORT", Boolean.class);

    /**
     * See {@link java.net.SocketOptions#SO_TIMEOUT}
     */
    Option<Integer> SO_TIMEOUT = new Option<>("SO_TIMEOUT", Integer.class);

    /**
     * See {@link java.net.SocketOptions#SO_REUSEADDR}
     */
    Option<Boolean> SO_REUSEADDR = new Option<>("SO_REUSEADDR", Boolean.class);

    /**
     * See {@link java.net.SocketOptions#TCP_NODELAY}
     */
    Option<Boolean> TCP_NODELAY = new Option<>("TCP_NODELAY", Boolean.class);

    /**
     * See {@code jdk.net.ExtendedSocketOptions#TCP_KEEPIDLE}
     */
    Option<Integer> TCP_KEEPIDLE = new Option<>("TCP_KEEPIDLE", Integer.class);

    /**
     * See {@code jdk.net.ExtendedSocketOptions#TCP_KEEPINTERVAL}
     */
    Option<Integer> TCP_KEEPINTERVAL = new Option<>("TCP_KEEPINTERVAL", Integer.class);

    /**
     * See {@code jdk.net.ExtendedSocketOptions#TCP_KEEPCOUNT}
     */
    Option<Integer> TCP_KEEPCOUNT = new Option<>("TCP_KEEPCOUNT", Integer.class);


    /**
     * Checks if the option is supported.
     *
     * @param option the option
     * @return true if supported, false otherwise
     * @throws NullPointerException if option is <code>null</code>.
     */
    boolean isSupported(Option option);

    /**
     * Sets an option value.
     *
     * @param option the option
     * @param value  the value
     * @param <T>    the type of the value
     * @throws NullPointerException          if option or value is null.
     * @throws UnsupportedOperationException if the option isn't supported.
     * @throws java.io.UncheckedIOException  if the value could not be set.
     */
    default <T> void set(Option<T> option, T value) {
        if (!setIfSupported(option, value)) {
            throw new UnsupportedOperationException("'" + option.name() + "' not supported");
        }
    }

    /**
     * Sets an option value if that option is supported.
     *
     * @param option the option
     * @param value  the value
     * @param <T>    the type of the value
     * @return true if the option was supported, false otherwise.
     * @throws NullPointerException         if option or value is null.
     * @throws java.io.UncheckedIOException if the value could not be set.
     */
    <T> boolean setIfSupported(Option<T> option, T value);

    /**
     * Gets an option value if that option is supported. If option not supported,
     * <code>null</code> is returned.
     *
     * @param option the option
     * @param <T>    the type of the value
     * @return null if value is null.
     * @throws java.io.UncheckedIOException if the value could not be get.
     */
    <T> T getIfSupported(Option<T> option);

    /**
     * Gets an option value.
     *
     * @param option the option
     * @param <T>    the type of the value
     * @return the value for the option
     * @throws UnsupportedOperationException if the option isn't supported.
     * @throws java.io.UncheckedIOException  if the value could not be get.
     */
    default <T> T get(Option<T> option) {
        T value = getIfSupported(option);
        if (value == null) {
            throw new UnsupportedOperationException("'" + option.name() + "' not supported");
        } else {
            return value;
        }
    }
}
