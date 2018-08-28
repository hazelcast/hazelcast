/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.networking.ChannelOptions;
import com.hazelcast.internal.networking.ChannelOption;

import java.net.Socket;
import java.net.SocketException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_KEEPALIVE;
import static com.hazelcast.internal.networking.ChannelOption.SO_LINGER;
import static com.hazelcast.internal.networking.ChannelOption.SO_RCVBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_REUSEADDR;
import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_TIMEOUT;
import static com.hazelcast.internal.networking.ChannelOption.TCP_NODELAY;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Contains the configuration for a {@link Channel}.
 */
final class NioChannelOptions implements ChannelOptions {

    private final Map<String, Object> values = new ConcurrentHashMap<String, Object>();
    private final Socket socket;

    NioChannelOptions(Socket socket) {
        setOption(DIRECT_BUF, false);
        this.socket = socket;
    }

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
            } else {
                return (T) values.get(option.name());
            }
        } catch (SocketException e) {
            throw new HazelcastException("Failed to getOption [" + option.name() + "]", e);
        }
    }

    @Override
    public <T> NioChannelOptions setOption(ChannelOption<T> option, T value) {
        checkNotNull(option, "option can't be null");
        checkNotNull(value, "value can't be null");

        try {
            if (option.equals(TCP_NODELAY)) {
                socket.setTcpNoDelay((Boolean) value);
            } else if (option.equals(SO_RCVBUF)) {
                socket.setReceiveBufferSize((Integer) value);
            } else if (option.equals(SO_SNDBUF)) {
                socket.setSendBufferSize((Integer) value);
            } else if (option.equals(SO_KEEPALIVE)) {
                socket.setKeepAlive((Boolean) value);
            } else if (option.equals(SO_REUSEADDR)) {
                socket.setReuseAddress((Boolean) value);
            } else if (option.equals(SO_TIMEOUT)) {
                socket.setSoTimeout((Integer) value);
            } else if (option.equals(SO_LINGER)) {
                int soLonger = (Integer) value;
                if (soLonger > 0) {
                    socket.setSoLinger(true, soLonger);
                }
            } else {
                values.put(option.name(), value);
            }
        } catch (SocketException e) {
            throw new HazelcastException("Failed to setOption [" + option.name() + "] with value [" + value + "]", e);
        }

        return this;
    }
}
