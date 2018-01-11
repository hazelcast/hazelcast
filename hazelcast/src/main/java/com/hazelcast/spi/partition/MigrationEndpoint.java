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

package com.hazelcast.spi.partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Denotes endpoint of a migration.
 */
public enum MigrationEndpoint {

    /**
     * Denotes old owner of a partition replica
     */
    SOURCE(0),

    /**
     * Denotes new owner of a partition replica
     */
    DESTINATION(1);

    private final byte code;

    MigrationEndpoint(int code) {
        this.code = (byte) code;
    }

    public static void writeTo(MigrationEndpoint endpoint, DataOutput out) throws IOException {
        out.writeByte(endpoint.code);
    }

    public static MigrationEndpoint readFrom(DataInput in) throws IOException {
        byte code = in.readByte();
        return code == 0 ? SOURCE : DESTINATION;
    }
}
