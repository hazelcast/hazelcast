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

import java.util.concurrent.Executor;

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

    Option<Object> SSL_ENGINE_FACTORY = new Option<>("SSL_ENGINE_FACTORY", Object.class);

    Option<Executor> TLS_EXECUTOR = new Option<>("TLS_EXECUTOR", Executor.class);


    /**
     * Checks if the option is supported.
     *
     * @param option the option
     * @return true if supported, false otherwise
     * @throws NullPointerException if option is <code>null</code>.
     */
    boolean isSupported(Option option);

    /**
     * Sets an option value if that option is supported.
     *
     * @param option the option
     * @param value  the value
     * @param <T>    the type of the value
     * @return <code>true</code> if the option was supported, <code>false</code> otherwise.
     * @throws NullPointerException          if option or value is null.
     * @throws java.io.UncheckedIOException  if the value could not be set.
     */

    <T> boolean set(Option<T> option, T value);

    /**
     * Gets an option value. If option was not set or is not supported, <code>null</code> is returned.
     *
     * @param option the option
     * @param <T>    the type of the value
     * @return the value for the option, <code>null</code> if the option was not set or is not supported.
     * @throws java.io.UncheckedIOException  if the value could not be gotten.
     */
    <T> T get(Option<T> option);
}
