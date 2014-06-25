/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.hibernate.local;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Hazelcast compatible implementation of a timestamp for internal eviction
 */
// TODO Make IdentifiedDataSerializable
public class Timestamp implements DataSerializable {

    private Object key;
    private long timestamp;

    public Timestamp() {
    }

    public Timestamp(final Object key, final long timestamp) {
        this.key = key;
        this.timestamp = timestamp;
    }

    public Object getKey() {
        return key;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeObject(key);
        out.writeLong(timestamp);
    }

    public void readData(final ObjectDataInput in) throws IOException {
        key = in.readObject();
        timestamp = in.readLong();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Timestamp");
        sb.append("{key=").append(key);
        sb.append(", timestamp=").append(timestamp);
        sb.append('}');
        return sb.toString();
    }
}
