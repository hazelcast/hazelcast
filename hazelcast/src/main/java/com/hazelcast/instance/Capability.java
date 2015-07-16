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

package com.hazelcast.instance;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;

/**
 * Defines the different capabilities a member can have in the cluster
 * A member with no capabilities has read and write access to the data in the cluster
 * @since 3.4
 */
public enum Capability {
    /**
     * The PARTITION_HOST capability makes a member eligible to host data partitions / replicas
     */
    PARTITION_HOST(0);

    private final int id;

    Capability(int id) {
        this.id = id;
    }

    public static Capability valueOf(int id) {
        for (Capability capability : values()) {
            if (capability.id == id) {
                return capability;
            }
        }
        throw new IllegalArgumentException(String.format("No capability with id [%d] could be found", id));
    }

    public static Set<Capability> readCapabilities(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        EnumSet<Capability> capabilities = EnumSet.noneOf(Capability.class);
        for (int i = 0; i < size; i++) {
            capabilities.add(valueOf(in.readInt()));
        }
        return capabilities;
    }

    public static void writeCapabilities(ObjectDataOutput out, Set<Capability> capabilities) throws IOException {
        out.writeInt(capabilities.size());
        for (Capability capability : capabilities) {
            out.writeInt(capability.id);
        }
    }
}
