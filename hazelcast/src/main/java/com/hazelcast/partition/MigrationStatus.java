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

package com.hazelcast.partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @mdogan 8/11/12
 */
public enum MigrationStatus {

    STARTED(0),
    COMPLETED(1),
    FAILED(-1);

    private final byte code;

    private MigrationStatus(final int code) {
        this.code = (byte) code;
    }

    public static void writeTo(MigrationStatus status, DataOutput out) throws IOException {
        out.writeByte(status.code);
    }

    public static MigrationStatus readFrom(DataInput in) throws IOException {
        final byte code = in.readByte();
        switch (code) {
            case 0:
                return STARTED;
            case 1:
                return COMPLETED;
            case -1:
                return FAILED;
        }
        return null;
    }
}
