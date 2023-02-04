/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc.nio;

import com.hazelcast.internal.tpc.AsyncSocketOptions;
import com.hazelcast.internal.tpc.Option;
import com.hazelcast.internal.tpc.logging.TpcLogger;
import com.hazelcast.internal.tpc.logging.TpcLoggerLocator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_RCVBUF;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_REUSEADDR;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_REUSEPORT;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpc.util.ReflectionUtil.findStaticFieldValue;

/**
 * The AsyncSocketOptions for the {@link NioAsyncServerSocket}.
 */
public class NioAsyncServerSocketOptions implements AsyncSocketOptions {

    private static final AtomicBoolean SO_REUSE_PORT_PRINTED = new AtomicBoolean();

    // This option is available since Java 9, so we need to use reflection.
    private static final SocketOption<Boolean> JAVA_NET_SO_REUSEPORT
            = findStaticFieldValue(StandardSocketOptions.class, "SO_REUSEPORT");

    private final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());

    private final ServerSocketChannel serverSocketChannel;

    NioAsyncServerSocketOptions(ServerSocketChannel serverSocketChannel) {
        this.serverSocketChannel = serverSocketChannel;
    }

    @Override
    public <T> void set(Option<T> option, T value) {
        checkNotNull(option, "option");
        checkNotNull(value, "value");

        try {
            if (SO_RCVBUF.equals(option)) {
                serverSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, (Integer) value);
            } else if (SO_REUSEADDR.equals(option)) {
                serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, (Boolean) value);
            } else if (SO_REUSEPORT.equals(option)) {
                if (JAVA_NET_SO_REUSEPORT == null) {
                    if (SO_REUSE_PORT_PRINTED.compareAndSet(false, true)) {
                        logger.warning("Ignoring SO_REUSEPORT."
                                + "Please upgrade to Java 9+ to enable the SO_REUSEPORT option.");
                    }
                } else {
                    serverSocketChannel.setOption(JAVA_NET_SO_REUSEPORT, (Boolean) value);
                }
            } else {
                throw new UnsupportedOperationException("Unrecognized option " + option);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to set " + option.name() + " with value [" + value + "]", e);
        }
    }

    @Override
    public <T> T get(Option<T> option) {
        checkNotNull(option, "option");

        try {
            if (SO_RCVBUF.equals(option)) {
                return (T) serverSocketChannel.getOption(StandardSocketOptions.SO_RCVBUF);
            } else if (SO_REUSEADDR.equals(option)) {
                return (T) serverSocketChannel.getOption(StandardSocketOptions.SO_REUSEADDR);
            } else if (SO_REUSEPORT.equals(option)) {
                if (JAVA_NET_SO_REUSEPORT == null) {
                    return (T) Boolean.FALSE;
                } else {
                    return (T) serverSocketChannel.getOption(JAVA_NET_SO_REUSEPORT);
                }
            } else {
                throw new UnsupportedOperationException("Unrecognized option:" + option);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to get option " + option.name(), e);
        }
    }
}
