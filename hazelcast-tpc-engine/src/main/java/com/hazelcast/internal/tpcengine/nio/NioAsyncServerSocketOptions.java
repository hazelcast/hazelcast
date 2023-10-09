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
import com.hazelcast.internal.tpcengine.util.OS;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.util.Set;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * The AsyncSocketOptions for the {@link NioAsyncServerSocket}.
 */
public class NioAsyncServerSocketOptions implements AsyncSocketOptions {

    private static final Set<SocketOption<?>> WINDOWS_UNSUPPORTED_OPTIONS = Set.of(StandardSocketOptions.SO_REUSEPORT);
    private final ServerSocketChannel serverSocketChannel;

    NioAsyncServerSocketOptions(ServerSocketChannel serverSocketChannel) {
        this.serverSocketChannel = serverSocketChannel;
    }

    private static SocketOption toSocketOption(Option option) {
        if (SO_RCVBUF.equals(option)) {
            return StandardSocketOptions.SO_RCVBUF;
        } else if (SO_REUSEADDR.equals(option)) {
            return StandardSocketOptions.SO_REUSEADDR;
        } else if (SO_REUSEPORT.equals(option)) {
            return StandardSocketOptions.SO_REUSEPORT;
        } else {
            return null;
        }
    }

    @Override
    public boolean isSupported(Option option) {
        checkNotNull(option, "option");

        SocketOption socketOption = toSocketOption(option);
        return isSupported(socketOption);
    }

    private boolean isSupported(SocketOption socketOption) {
        if (socketOption == null) {
            return false;
        }
        if (OS.isWindows() && WINDOWS_UNSUPPORTED_OPTIONS.contains(socketOption)) {
            return false;
        }
        return serverSocketChannel.supportedOptions().contains(socketOption);
    }

    @Override
    public <T> boolean set(Option<T> option, T value) {
        checkNotNull(option, "option");
        checkNotNull(value, "value");

        try {
            SocketOption socketOption = toSocketOption(option);
            if (isSupported(socketOption)) {
                serverSocketChannel.setOption(socketOption, value);
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to set " + option.name() + " with value [" + value + "]", e);
        }
    }

    @Override
    public <T> T get(Option<T> option) {
        checkNotNull(option, "option");

        try {
            SocketOption socketOption = toSocketOption(option);
            if (isSupported(socketOption)) {
                return (T) serverSocketChannel.getOption(socketOption);
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to get option " + option.name(), e);
        }
    }
}
