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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.EventFilter;

import java.io.IOException;

/**
 * Compares supplied UUID with this filters' UUID to prevent unneeded delivery of an invalidation event to operation caller.
 * Operation caller invalidates its own local Near Cache, no need to send an extra invalidation from remote.
 */
public class UuidFilter implements EventFilter, IdentifiedDataSerializable {

    private String uuid;

    public UuidFilter() {
    }

    public UuidFilter(String uuid) {
        this.uuid = uuid;
    }

    @Override
    public boolean eval(Object suppliedUuid) {
        assert suppliedUuid instanceof String;

        return uuid.equals(suppliedUuid);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(uuid);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        uuid = in.readUTF();
    }

    @Override
    public String toString() {
        return "UuidFilter{"
                + "uuid='" + uuid + '\''
                + '}';
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.UUID_FILTER;
    }
}
