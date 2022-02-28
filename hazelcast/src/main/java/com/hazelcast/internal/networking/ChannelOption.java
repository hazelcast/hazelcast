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

package com.hazelcast.internal.networking;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * A {@link ChannelOptions} option for the {@link ChannelOptions}.
 *
 * @param <T> the type of the value for the option.
 */
public final class ChannelOption<T> {

    /**
     * See {@link java.net.SocketOptions#SO_RCVBUF}.
     */
    public static final ChannelOption<Integer> SO_RCVBUF = new ChannelOption<Integer>("SO_RCVBUF");

    /**
     * See {@link java.net.SocketOptions#SO_SNDBUF}
     */
    public static final ChannelOption<Integer> SO_SNDBUF = new ChannelOption<Integer>("SO_SNDBUF");

    /**
     * See {@link java.net.SocketOptions#SO_KEEPALIVE}
     */
    public static final ChannelOption<Boolean> SO_KEEPALIVE = new ChannelOption<Boolean>("SO_KEEPALIVE");

    /**
     * See {@link java.net.SocketOptions#SO_LINGER}
     */
    public static final ChannelOption<Integer> SO_LINGER = new ChannelOption<Integer>("SO_LINGER");

    /**
     * See {@link java.net.SocketOptions#SO_TIMEOUT}
     */
    public static final ChannelOption<Integer> SO_TIMEOUT = new ChannelOption<Integer>("SO_TIMEOUT");

    /**
     * See {@link java.net.SocketOptions#SO_REUSEADDR}
     */
    public static final ChannelOption<Boolean> SO_REUSEADDR = new ChannelOption<Boolean>("SO_REUSEADDR");

    /**
     * See {@link java.net.SocketOptions#TCP_NODELAY}
     */
    public static final ChannelOption<Boolean> TCP_NODELAY = new ChannelOption<Boolean>("TCP_NODELAY");

    /**
     * If a direct buffer should be used or a regular buffer.
     * See {@link java.nio.ByteBuffer#allocateDirect(int)}
     */
    public static final ChannelOption<Boolean> DIRECT_BUF = new ChannelOption<Boolean>("DIRECT_BUF");

    private final String name;

    /**
     * Creates a ChannelOption with the provided name.
     *
     * @param name the name of the ChannelOption
     */
    public ChannelOption(String name) {
        this.name = checkNotNull(name, "name can't be null");
    }

    /**
     * Returns the name
     *
     * @return the name.
     */
    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ChannelOption<?> that = (ChannelOption<?>) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
