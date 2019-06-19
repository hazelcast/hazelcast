/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.raft.impl.testing;

import com.hazelcast.cluster.Endpoint;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class TestRaftMember implements Endpoint {

    private String uuid;

    private int port;

    public TestRaftMember(String uuid, int port) {
        this.uuid = uuid;
        this.port = port;
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    @Override
    public SocketAddress getSocketAddress() {
        return new InetSocketAddress(port);
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TestRaftMember that = (TestRaftMember) o;

        if (port != that.port) {
            return false;
        }
        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        int result = uuid.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return "TestRaftMember{" + "uuid='" + uuid + '\'' + ", port=" + port + '}';
    }

}
