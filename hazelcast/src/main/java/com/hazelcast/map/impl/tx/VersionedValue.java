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

package com.hazelcast.map.impl.tx;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import java.io.IOException;

/**
 * Wrapper for {@link com.hazelcast.nio.serialization.Data} value objects with version information.
 */
public class VersionedValue implements DataSerializable {
    long version;
    Data value;

    public VersionedValue(Data value, long version) {
        this.value = value;
        this.version = version;
    }

    public VersionedValue() {
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(version);
        boolean isNull = value == null;
        out.writeBoolean(isNull);
        if (!isNull) {
            value.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        version = in.readLong();
        boolean isNull = in.readBoolean();
        if (!isNull) {
            value = new Data();
            value.readData(in);
        }
    }
}
