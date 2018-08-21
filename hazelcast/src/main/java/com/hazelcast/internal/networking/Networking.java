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

import com.hazelcast.internal.networking.nio.NioNetworking;

import java.io.IOException;
import java.nio.channels.SocketChannel;

/**
 * The Networking is an abstraction responsible for lower level networking services.
 *
 * Networking is based on a set of registered channel and
 *
 * The default implementation of this is {@link NioNetworking} that relies on
 * selectors. But also different implementations can be added like spinning,
 * thread per connection, epoll, UDP based etc.
 *
 * @see NioNetworking
 */
public interface Networking {

    /**
     * Registers the SocketChannel to the EventLoop group and returns the
     * created Channel.
     *
     * The Channel is not yet started so that modifications can be made to the
     * channel e.g. adding attributes. Once this is done the {@link Channel#start()}
     * needs to be called.
     *
     * In the future we need to think about passing the socket channel because
     * it binds Networking to tcp and this is not desirable.
     *
     * @param socketChannel the socketChannel to register
     * @param clientMode    if the channel is made in clientMode or server mode
     * @return the created Channel
     * @throws IOException when something failed while registering the
     *                     socketChannel
     */
    Channel register(SocketChannel socketChannel, boolean clientMode) throws IOException;

    /**
     * Starts Networking.
     */
    void start();

    /**
     * Shuts down Networking.
     */
    void shutdown();
}
