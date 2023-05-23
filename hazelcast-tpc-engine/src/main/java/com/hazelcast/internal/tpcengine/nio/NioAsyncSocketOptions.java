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

package com.hazelcast.internal.tpcengine.nio;


import com.hazelcast.internal.tpcengine.Option;
import com.hazelcast.internal.tpcengine.net.AsyncSocketOptions;
import jdk.net.ExtendedSocketOptions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

@SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:returncount", "java:S3776"})
public class NioAsyncSocketOptions implements AsyncSocketOptions {

    private final SocketChannel socketChannel;
    private final Map<Option, Object> extraOptions = new HashMap<>();

    NioAsyncSocketOptions(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    private static SocketOption toSocketOption(Option option) {
        if (TCP_NODELAY.equals(option)) {
            return StandardSocketOptions.TCP_NODELAY;
        } else if (SO_RCVBUF.equals(option)) {
            return StandardSocketOptions.SO_RCVBUF;
        } else if (SO_SNDBUF.equals(option)) {
            return StandardSocketOptions.SO_SNDBUF;
        } else if (SO_KEEPALIVE.equals(option)) {
            return StandardSocketOptions.SO_KEEPALIVE;
        } else if (SO_REUSEADDR.equals(option)) {
            return StandardSocketOptions.SO_REUSEADDR;
        } else if (TCP_KEEPCOUNT.equals(option)) {
            return ExtendedSocketOptions.TCP_KEEPCOUNT;
        } else if (TCP_KEEPINTERVAL.equals(option)) {
            return ExtendedSocketOptions.TCP_KEEPINTERVAL;
        } else if (TCP_KEEPIDLE.equals(option)) {
            return ExtendedSocketOptions.TCP_KEEPIDLE;
        } else {
            return null;
        }
    }

    @Override
    public boolean isSupported(Option option) {
        checkNotNull(option, "option");

        return isSupported(toSocketOption(option)) || SSL_ENGINE_FACTORY.equals(option) || TLS_EXECUTOR.equals(option);
    }

    private boolean isSupported(SocketOption socketOption) {
        return socketOption != null && socketChannel.supportedOptions().contains(socketOption);
    }

    @Override
    public <T> T get(Option<T> option) {
        checkNotNull(option, "option");

        try {
            if (!isSupported(option)) {
                return null;
            }

            SocketOption socketOption = toSocketOption(option);
            if (isSupported(socketOption)) {
                return (T) socketChannel.getOption(socketOption);
            }

            return (T) extraOptions.get(option);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public <T> boolean set(Option<T> option, T value) {
        checkNotNull(option, "option");
        checkNotNull(value, "value");

        try {
            if (!isSupported(option)) {
                return false;
            }

            SocketOption socketOption = toSocketOption(option);
            if (isSupported(socketOption)) {
                socketChannel.setOption(socketOption, value);
                return true;
            }

            extraOptions.put(option, value);
            return true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
