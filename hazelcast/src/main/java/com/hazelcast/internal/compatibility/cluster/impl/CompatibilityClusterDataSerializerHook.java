/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.compatibility.cluster.impl;

import com.hazelcast.internal.compatibility.version.CompatibilityMemberVersion;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.version.Version;

/**
 * Data serializer hook containing (de)serialization information for communicating
 * with 4.x members over WAN.
 */
public final class CompatibilityClusterDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = 0;

    public static final int ADDRESS = 1;
    public static final int BIND_MESSAGE = 5;
    public static final int MEMBER_VERSION = 29;
    public static final int VERSION = 32;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case ADDRESS:
                        return new Address();
                    case VERSION:
                        return new Version();
                    case BIND_MESSAGE:
                        return new CompatibilityBindMessage();
                    case MEMBER_VERSION:
                        return new CompatibilityMemberVersion();
                    default:
                        return null;
                }
            }
        };
    }
}
