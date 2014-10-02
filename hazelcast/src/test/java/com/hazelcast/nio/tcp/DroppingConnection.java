/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.SocketWritable;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

class DroppingConnection implements Connection {

    final Address endpoint;
    final long timestamp = Clock.currentTimeMillis();

    DroppingConnection(Address endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public boolean write(SocketWritable packet) {
        return true;
    }

    @Override
    public boolean live() {
        return true;
    }

    @Override
    public long lastReadTime() {
        return timestamp;
    }

    @Override
    public long lastWriteTime() {
        return timestamp;
    }

    @Override
    public void close() {
    }

    @Override
    public ConnectionType getType() {
        return ConnectionType.MEMBER;
    }

    @Override
    public boolean isClient() {
        return false;
    }

    @Override
    public InetAddress getInetAddress() {
        try {
            return endpoint.getInetAddress();
        } catch (UnknownHostException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        try {
            return endpoint.getInetSocketAddress();
        } catch (UnknownHostException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Address getEndPoint() {
        return endpoint;
    }

    @Override
    public int getPort() {
        return endpoint.getPort();
    }
}
