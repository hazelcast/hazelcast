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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.instance.EndpointQualifier;

import java.nio.channels.ServerSocketChannel;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.nio.IOUtil.closeResource;

/**
 * Registry that holds all initiated ServerSocket when advanced-networking is in-use
 * Or holds a single universal ServerSocket when plain old networking is in-use.
 *
 * An atomic flag is used to determine whether the registry is open and it hasn't been destroyed / released.
 */
public class ServerSocketRegistry
        implements Iterable<ServerSocketRegistry.Pair> {

    private final boolean unifiedSocket;
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final Map<EndpointQualifier, ServerSocketChannel> serverSocketChannelMap;
    private final Set<Pair> entries = new HashSet<Pair>();

    public ServerSocketRegistry(Map<EndpointQualifier, ServerSocketChannel> map, boolean unifiedSocket) {
        this.unifiedSocket = unifiedSocket;
        this.serverSocketChannelMap = map == null
                ? Collections.<EndpointQualifier, ServerSocketChannel>emptyMap()
                : map;
        buildEntries();
    }

    public boolean isOpen() {
        return isOpen.get();
    }

    public boolean holdsUnifiedSocket() {
        return unifiedSocket;
    }

    private void buildEntries() {
        if (!serverSocketChannelMap.isEmpty()) {
            for (Map.Entry<EndpointQualifier, ServerSocketChannel> entry : serverSocketChannelMap.entrySet()) {
                entries.add(new Pair(entry.getValue(), entry.getKey()));
            }
        }
    }

    @Override
    public Iterator<Pair> iterator() {
        return entries.iterator();
    }

    public static final class Pair {
        private final ServerSocketChannel channel;
        private final EndpointQualifier qualifier;

        private Pair(ServerSocketChannel channel, EndpointQualifier qualifier) {
            this.channel = channel;
            this.qualifier = qualifier;
        }

        public ServerSocketChannel getChannel() {
            return channel;
        }

        public EndpointQualifier getQualifier() {
            return qualifier;
        }
    }

    public void destroy() {
        if (isOpen.compareAndSet(true, false)) {
            for (ServerSocketChannel channel : serverSocketChannelMap.values()) {
                closeResource(channel);
            }
        }
    }

    @Override
    public String toString() {
        return "ServerSocketRegistry{" + serverSocketChannelMap + "}";
    }
}
