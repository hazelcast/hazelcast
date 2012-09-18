/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

/**
 * @mdogan 8/11/12
 */
public enum MigrationStatus {

    STARTED(0),
    COMPLETED(1),
    FAILED(-1);

    public static MigrationStatus get(byte code) {
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

    private final byte code;

    private MigrationStatus(final int code) {
        this.code = (byte) code;
    }

    public byte getCode() {
        return code;
    }
}
