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
import jdk.net.ExtendedSocketOptions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpc.util.ReflectionUtil.findStaticFieldValue;

@SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:returncount"})
public class NioAsyncSocketOptions implements AsyncSocketOptions {

    private static final java.net.SocketOption<Integer> JDK_NET_TCP_KEEPCOUNT
            = findStaticFieldValue(ExtendedSocketOptions.class, "TCP_KEEPCOUNT");
    private static final java.net.SocketOption<Integer> JDK_NET_TCP_KEEPIDLE
            = findStaticFieldValue(ExtendedSocketOptions.class, "TCP_KEEPIDLE");
    private static final java.net.SocketOption<Integer> JDK_NET_TCP_KEEPINTERVAL
            = findStaticFieldValue(ExtendedSocketOptions.class, "TCP_KEEPINTERVAL");

    private static final AtomicBoolean TCP_KEEPCOUNT_PRINTED = new AtomicBoolean();
    private static final AtomicBoolean TCP_KEEPIDLE_PRINTED = new AtomicBoolean();
    private static final AtomicBoolean TCP_KEEPINTERVAL_PRINTED = new AtomicBoolean();

    private final SocketChannel channel;
    private final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());

    NioAsyncSocketOptions(SocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public <T> T get(Option<T> option) {
        checkNotNull(option, "option");

        try {
            if (TCP_NODELAY.equals(option)) {
                return (T) channel.getOption(StandardSocketOptions.TCP_NODELAY);
            } else if (SO_RCVBUF.equals(option)) {
                return (T) channel.getOption(StandardSocketOptions.SO_RCVBUF);
            } else if (SO_SNDBUF.equals(option)) {
                return (T) channel.getOption(StandardSocketOptions.SO_SNDBUF);
            } else if (SO_KEEPALIVE.equals(option)) {
                return (T) channel.getOption(StandardSocketOptions.SO_KEEPALIVE);
            } else if (SO_REUSEADDR.equals(option)) {
                return (T) channel.getOption(StandardSocketOptions.SO_REUSEADDR);
            } else if (SO_TIMEOUT.equals(option)) {
                return (T) (Integer) channel.socket().getSoTimeout();
            } else if (TCP_KEEPCOUNT.equals(option)) {
                if (JDK_NET_TCP_KEEPCOUNT == null) {
                    return (T) Integer.valueOf(0);
                } else {
                    return (T) channel.getOption(JDK_NET_TCP_KEEPCOUNT);
                }
            } else if (TCP_KEEPINTERVAL.equals(option)) {
                if (JDK_NET_TCP_KEEPINTERVAL == null) {
                    return (T) Integer.valueOf(0);
                } else {
                    return (T) channel.getOption(JDK_NET_TCP_KEEPINTERVAL);
                }
            } else if (TCP_KEEPIDLE.equals(option)) {
                if (JDK_NET_TCP_KEEPIDLE == null) {
                    return (T) Integer.valueOf(0);
                } else {
                    return (T) channel.getOption(JDK_NET_TCP_KEEPIDLE);
                }
            } else {
                throw new UnsupportedOperationException("Unrecognized option:" + option);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    @Override
    public <T> void set(Option<T> option, T value) {
        checkNotNull(option, "option");
        checkNotNull(value, "value");

        try {
            if (TCP_NODELAY.equals(option)) {
                channel.setOption(StandardSocketOptions.TCP_NODELAY, (Boolean) value);
            } else if (SO_RCVBUF.equals(option)) {
                channel.setOption(StandardSocketOptions.SO_RCVBUF, (Integer) value);
            } else if (SO_SNDBUF.equals(option)) {
                channel.setOption(StandardSocketOptions.SO_SNDBUF, (Integer) value);
            } else if (SO_KEEPALIVE.equals(option)) {
                channel.setOption(StandardSocketOptions.SO_KEEPALIVE, (Boolean) value);
            } else if (SO_REUSEADDR.equals(option)) {
                channel.setOption(StandardSocketOptions.SO_REUSEADDR, (Boolean) value);
            } else if (SO_TIMEOUT.equals(option)) {
                channel.socket().setSoTimeout((Integer) value);
            } else if (TCP_KEEPCOUNT.equals(option)) {
                if (JDK_NET_TCP_KEEPCOUNT == null) {
                    if (TCP_KEEPCOUNT_PRINTED.compareAndSet(false, true)) {
                        logger.warning("Ignoring TCP_KEEPCOUNT. "
                                + "Please upgrade to Java 11+ or configure tcp_keepalive_probes in the kernel: "
                                + "For more info see https://tldp.org/HOWTO/html_single/TCP-Keepalive-HOWTO/. "
                                + "If this isn't dealt with, idle connections could be closed prematurely.");
                    }
                } else {
                    channel.setOption(JDK_NET_TCP_KEEPCOUNT, (Integer) value);
                }
            } else if (TCP_KEEPIDLE.equals(option)) {
                if (JDK_NET_TCP_KEEPIDLE == null) {
                    if (TCP_KEEPIDLE_PRINTED.compareAndSet(false, true)) {
                        logger.warning("Ignoring TCP_KEEPIDLE. "
                                + "Please upgrade to Java 11+ or configure tcp_keepalive_time in the kernel. "
                                + "For more info see https://tldp.org/HOWTO/html_single/TCP-Keepalive-HOWTO/. "
                                + "If this isn't dealt with, idle connections could be closed prematurely.");
                    }
                } else {
                    channel.setOption(JDK_NET_TCP_KEEPIDLE, (Integer) value);
                }
            } else if (TCP_KEEPINTERVAL.equals(option)) {
                if (JDK_NET_TCP_KEEPINTERVAL == null) {
                    if (TCP_KEEPINTERVAL_PRINTED.compareAndSet(false, true)) {
                        logger.warning("Ignoring TCP_KEEPINTERVAL. "
                                + "Please upgrade to Java 11+ or configure tcp_keepalive_intvl in the kernel. "
                                + "For more info see https://tldp.org/HOWTO/html_single/TCP-Keepalive-HOWTO/. "
                                + "If this isn't dealt with, idle connections could be closed prematurely.");
                    }
                } else {
                    channel.setOption(JDK_NET_TCP_KEEPINTERVAL, (Integer) value);
                }
            } else {
                throw new UnsupportedOperationException("Unrecognized option:" + option);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to setOption [" + option.name() + "] with value [" + value + "]", e);
        }
    }
}
