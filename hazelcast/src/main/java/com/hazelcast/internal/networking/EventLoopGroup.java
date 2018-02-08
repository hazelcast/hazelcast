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

package com.hazelcast.internal.networking;

import com.hazelcast.internal.networking.nio.NioEventLoopGroup;

import java.io.IOException;
import java.nio.channels.SocketChannel;

/**
 * The EventLoopGroup is responsible for processing registered channels.
 * Effectively it is the threading model for the io system.
 *
 * An event loop is for example visible on the NioThread where we loop over the
 * selector events. The EventLoopGroup is the group of all these thread
 * instances.
 *
 * The default implementation of this is the {@link NioEventLoopGroup} that
 * relies on selectors. But also different implementations can be added like
 * spinning, thread per connection, epoll based etc.
 *
 * @see NioEventLoopGroup
 */
public interface EventLoopGroup {

    /**
     * Registers the SocketChannel to the EventLoop group and returns the
     * created Channel.
     *
     * The Channel is not yet started so that modifications can be made to the
     * channel e.g. adding attributes. Once this is done the {@link Channel#start()}
     * needs to be called.
     *
     * @param socketChannel the socketChannel to register
     * @param clientMode if the channel is made in clientMode or server mode
     * @return the created Channel
     * @throws IOException
     */
    Channel register(SocketChannel socketChannel, boolean clientMode) throws IOException;

    /**
     * Starts this EventLoopGroup.
     */
    void start();

    /**
     * Shuts down this EventLoopGroup.
     */
    void shutdown();
}
