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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelOption;
import com.hazelcast.internal.networking.ChannelOptions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketOption;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_KEEPALIVE;
import static com.hazelcast.internal.networking.ChannelOption.SO_LINGER;
import static com.hazelcast.internal.networking.ChannelOption.SO_RCVBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_REUSEADDR;
import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_TIMEOUT;
import static com.hazelcast.internal.networking.ChannelOption.TCP_KEEPCOUNT;
import static com.hazelcast.internal.networking.ChannelOption.TCP_KEEPIDLE;
import static com.hazelcast.internal.networking.ChannelOption.TCP_KEEPINTERVAL;
import static com.hazelcast.internal.networking.ChannelOption.TCP_NODELAY;
import static com.hazelcast.internal.tpc.util.ReflectionUtil.findStaticFieldValue;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.logging.Level.WARNING;

/**
 * Contains the configuration for a {@link Channel}.
 */
public final class NioChannelOptions implements ChannelOptions {

    public static final SocketOption<Integer> JDK_NET_TCP_KEEPCOUNT
            = findStaticFieldValue("jdk.net.ExtendedSocketOptions", "TCP_KEEPCOUNT");
    public static final SocketOption<Integer> JDK_NET_TCP_KEEPIDLE
                    = findStaticFieldValue("jdk.net.ExtendedSocketOptions", "TCP_KEEPIDLE");
    public static final SocketOption<Integer> JDK_NET_TCP_KEEPINTERVAL
                            = findStaticFieldValue("jdk.net.ExtendedSocketOptions", "TCP_KEEPINTERVAL");
    private static final AtomicBoolean TCP_KEEPCOUNT_WARNING = new AtomicBoolean();
    private static final AtomicBoolean TCP_KEEPIDLE_WARNING = new AtomicBoolean();
    private static final AtomicBoolean TCP_KEEPINTERVAL_WARNING = new AtomicBoolean();
    private static final AtomicBoolean SEND_BUFFER_WARNING = new AtomicBoolean();
    private static final AtomicBoolean RECEIVE_BUFFER_WARNING = new AtomicBoolean();

    private final Map<String, Object> values = new ConcurrentHashMap<>();
    private final SocketChannel socketChannel;
    private final Socket socket;
    private final ILogger logger;

    NioChannelOptions(SocketChannel socketChannel, ILogger logger) {
        setOption(DIRECT_BUF, false);
        this.socketChannel = socketChannel;
        this.socket = socketChannel.socket();
        this.logger = logger;
    }

    @SuppressWarnings("checkstyle:returncount")
    @Override
    public <T> T getOption(ChannelOption<T> option) {
        try {
            if (option.equals(TCP_NODELAY)) {
                return (T) (Boolean) socket.getTcpNoDelay();
            } else if (option.equals(SO_RCVBUF)) {
                return (T) (Integer) socket.getReceiveBufferSize();
            } else if (option.equals(SO_SNDBUF)) {
                return (T) (Integer) socket.getSendBufferSize();
            } else if (option.equals(SO_KEEPALIVE)) {
                return (T) (Boolean) socket.getKeepAlive();
            } else if (option.equals(SO_REUSEADDR)) {
                return (T) (Boolean) socket.getReuseAddress();
            } else if (option.equals(SO_TIMEOUT)) {
                return (T) (Integer) socket.getSoTimeout();
            } else if (option.equals(SO_LINGER)) {
                return (T) (Integer) socket.getSoLinger();
            } else if (option.equals(TCP_KEEPCOUNT)) {
                return (T) checkAndGetIntegerOption(JDK_NET_TCP_KEEPCOUNT);
            }  else if (option.equals(TCP_KEEPIDLE)) {
                return (T) checkAndGetIntegerOption(JDK_NET_TCP_KEEPIDLE);
            }  else if (option.equals(TCP_KEEPINTERVAL)) {
                return (T) checkAndGetIntegerOption(JDK_NET_TCP_KEEPINTERVAL);
            } else {
                return (T) values.get(option.name());
            }
        } catch (IOException e) {
            throw new HazelcastException("Failed to getOption [" + option.name() + "]", e);
        }
    }

    private Integer checkAndGetIntegerOption(SocketOption<Integer> socketOption) throws IOException {
        if (socketOption == null) {
            return 0;
        }
        return socketChannel.getOption(socketOption);
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:methodlength"})
    @Override
    public <T> NioChannelOptions setOption(ChannelOption<T> option, T value) {
        checkNotNull(option, "option can't be null");
        checkNotNull(value, "value can't be null");

        try {
            if (option.equals(TCP_NODELAY)) {
                socket.setTcpNoDelay((Boolean) value);
            } else if (option.equals(SO_RCVBUF)) {
                int receiveBufferSize = (Integer) value;
                socket.setReceiveBufferSize(receiveBufferSize);
                verifyReceiveBufferSize(receiveBufferSize);
            } else if (option.equals(SO_SNDBUF)) {
                int sendBufferSize = (Integer) value;
                socket.setSendBufferSize(sendBufferSize);
                verifySendBufferSize(sendBufferSize);
            } else if (option.equals(SO_KEEPALIVE)) {
                socket.setKeepAlive((Boolean) value);
            } else if (option.equals(SO_REUSEADDR)) {
                socket.setReuseAddress((Boolean) value);
            } else if (option.equals(SO_TIMEOUT)) {
                socket.setSoTimeout((Integer) value);
            } else if (option.equals(SO_LINGER)) {
                int soLinger = (Integer) value;
                if (soLinger >= 0) {
                    socket.setSoLinger(true, soLinger);
                }
            } else if (option.equals(TCP_KEEPCOUNT)) {
              if (JDK_NET_TCP_KEEPCOUNT == null) {
                  if (TCP_KEEPCOUNT_WARNING.compareAndSet(false, true)) {
                      logger.warning("Ignoring TCP_KEEPCOUNT. "
                              + "Please upgrade to the latest Java 8 release or Java 11+ to allow setting keep-alive count. "
                              + "Alternatively, on Linux, configure tcp_keepalive_probes in the kernel "
                              + "(affecting default keep-alive configuration for all sockets): "
                              + "For more info see https://tldp.org/HOWTO/html_single/TCP-Keepalive-HOWTO/. "
                              + "If this isn't dealt with, idle connections could be closed prematurely.");
                  }
              } else {
                  socketChannel.setOption(JDK_NET_TCP_KEEPCOUNT, (Integer) value);
              }
            } else if (option.equals(TCP_KEEPIDLE)) {
              if (JDK_NET_TCP_KEEPIDLE == null) {
                  if (TCP_KEEPIDLE_WARNING.compareAndSet(false, true)) {
                      logger.warning("Ignoring TCP_KEEPIDLE. "
                              + "Please upgrade to the latest Java 8 release or Java 11+ to allow setting keep-alive idle time."
                              + "Alternatively, on Linux, configure tcp_keepalive_time in the kernel "
                              + "(affecting default keep-alive configuration for all sockets): "
                              + "For more info see https://tldp.org/HOWTO/html_single/TCP-Keepalive-HOWTO/. "
                              + "If this isn't dealt with, idle connections could be closed prematurely.");
                  }
              } else {
                  socketChannel.setOption(JDK_NET_TCP_KEEPIDLE, (Integer) value);
              }
            } else if (option.equals(TCP_KEEPINTERVAL)) {
              if (JDK_NET_TCP_KEEPINTERVAL == null) {
                  if (TCP_KEEPINTERVAL_WARNING.compareAndSet(false, true)) {
                      logger.warning("Ignoring TCP_KEEPINTERVAL. "
                              + "Please upgrade to the latest Java 8 release or Java 11+ to allow setting keep-alive interval."
                              + "Alternatively, on Linux, configure tcp_keepalive_intvl in the kernel "
                              + "(affecting default keep-alive configuration for all sockets): "
                              + "For more info see https://tldp.org/HOWTO/html_single/TCP-Keepalive-HOWTO/. "
                              + "If this isn't dealt with, idle connections could be closed prematurely.");
                  }
              } else {
                  socketChannel.setOption(JDK_NET_TCP_KEEPINTERVAL, (Integer) value);
              }
            } else {
                values.put(option.name(), value);
            }
        } catch (IOException e) {
            throw new HazelcastException("Failed to setOption [" + option.name() + "] with value [" + value + "]", e);
        }

        return this;
    }

    private void verifySendBufferSize(int sendBufferSize) throws SocketException {
        if (socket.getSendBufferSize() == sendBufferSize) {
            return;
        }

        if (SEND_BUFFER_WARNING.compareAndSet(false, true)) {
            Logger.getLogger(NioChannelOptions.class).log(WARNING,
                    "The configured tcp send buffer size conflicts with the value actually being used "
                            + "by the socket and can lead to sub-optimal performance. "
                            + "Configured " + sendBufferSize + " bytes, "
                            + "actual " + socket.getSendBufferSize() + " bytes. "
                            + "On Linux look for kernel parameters 'net.ipv4.tcp_wmem' and 'net.core.wmem_max'."
                            + "This warning will only be shown once.");
        }

    }

    private void verifyReceiveBufferSize(int receiveBufferSize) throws SocketException {
        if (socket.getReceiveBufferSize() == receiveBufferSize) {
            return;
        }

        if (RECEIVE_BUFFER_WARNING.compareAndSet(false, true)) {
            Logger.getLogger(NioChannelOptions.class).log(WARNING,
                    "The configured tcp receive buffer size conflicts with the value actually being used "
                            + "by the socket and can lead to sub-optimal performance. "
                            + "Configured " + receiveBufferSize + " bytes, "
                            + "actual " + socket.getReceiveBufferSize() + " bytes. "
                            + "On Linux look for kernel parameters 'net.ipv4.tcp_rmem' and 'net.core.rmem_max'."
                            + "This warning will only be shown once.");
        }
    }
}
