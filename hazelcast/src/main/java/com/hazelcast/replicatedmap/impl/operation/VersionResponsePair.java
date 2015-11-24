/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import java.io.IOException;

/**
 * Contains response and partition version for update operations on replicated map.
 */
public class VersionResponsePair implements DataSerializable {
    Object response;
    long version;

    public VersionResponsePair() {
    }

    public VersionResponsePair(Object response, long version) {
        this.response = response;
        this.version = version;
    }

    public Object getResponse() {
        return response;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        final boolean isData = response instanceof Data;
        out.writeBoolean(isData);
        if (isData) {
            out.writeData((Data) response);
        } else {
            out.writeObject(response);
        }
        out.writeLong(version);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        final boolean isData = in.readBoolean();
        if (isData) {
            response = in.readData();
        } else {
            response = in.readObject();
        }
        version = in.readLong();
    }
}
