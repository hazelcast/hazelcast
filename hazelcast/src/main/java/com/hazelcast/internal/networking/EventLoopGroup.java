/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.networking.spinning.SpinningEventLoopGroup;

/**
 * An abstract of the threading model.
 *
 * The default implementation of this is the {@link NioEventLoopGroup} that relies on selectors. But also different
 * implementations can be added like spinning, thread per connection etc.
 *
 * @see NioEventLoopGroup
 * @see SpinningEventLoopGroup
 */
public interface EventLoopGroup {

    /**
     * Registers the Channel at this EventLoopGroup and the channel; so waiting for data to be send or received.
     *
     * @param channel the channel to register.
     * @throws NullPointerException if channel is null
     */
    void register(Channel channel);

    /**
     * Tells whether or not every I/O operation on SocketChannel should block until it completes.
     *
     * @return true if blocking, false otherwise.
     * @see {@link java.nio.channels.SelectableChannel#configureBlocking(boolean)}
     */
    boolean isBlocking();

    /**
     * Starts the EventLoopGroup.
     */
    void start();

    /**
     * Shuts down the EventLoopGroup.
     */
    void shutdown();
}
