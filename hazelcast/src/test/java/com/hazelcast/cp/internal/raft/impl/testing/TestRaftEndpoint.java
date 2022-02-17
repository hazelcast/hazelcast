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

package com.hazelcast.cp.internal.raft.impl.testing;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;

/**
 * Represents an endpoint that runs the Raft consensus algorithm as a member of
 * a Raft group.
 */
public class TestRaftEndpoint implements RaftEndpoint, DataSerializable {

    private UUID uuid;

    // We have port here for logging
    private int port;

    public TestRaftEndpoint() {
    }

    public TestRaftEndpoint(UUID uuid, int port) {
        this.uuid = uuid;
        this.port = port;
    }

    @Override
    public UUID getUuid() {
        return uuid;
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

        TestRaftEndpoint that = (TestRaftEndpoint) o;

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
        return "CPEndpoint{" + "uuid='" + uuid + '\'' + ", port=" + port + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        writeUUID(out, uuid);
        out.writeInt(port);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        uuid = readUUID(in);
        port = in.readInt();
    }

}
