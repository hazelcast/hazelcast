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

package com.hazelcast.impl;

import com.hazelcast.nio.Data;

public interface Constants {

    interface Objects {

        public static final Object OBJECT_DONE = new Object() {
            @Override
            public String toString() {
                return "OBJECT_DONE";
            }
        };

        public static final Object OBJECT_NULL = new Object() {
            @Override
            public String toString() {
                return "OBJECT_NULL";
            }
        };

        public static final Object OBJECT_NO_RESPONSE = new Object() {
            @Override
            public String toString() {
                return "OBJECT_NO_RESPONSE";
            }
        };

        public static final Object OBJECT_CANCELLED = new Object() {
            @Override
            public String toString() {
                return "OBJECT_CANCELLED";
            }
        };

        public static final Object OBJECT_MEMBER_LEFT = new Object() {
            @Override
            public String toString() {
                return "OBJECT_MEMBER_LEFT";
            }
        };

        public static final Object OBJECT_REDO = new Object() {
            @Override
            public String toString() {
                return "OBJECT_REDO";
            }
        };
    }

    interface IO {

        public static final int KILO_BYTE = 1024;

        public static final Data EMPTY_DATA = new Data();
    }

    public interface ResponseTypes {

        public static final byte RESPONSE_NONE = 2;

        public static final byte RESPONSE_SUCCESS = 3;

        public static final byte RESPONSE_FAILURE = 4;

        public static final byte RESPONSE_REDO = 5;
    }

    public enum RedoType {
        // using ordinal as redo type code
        // !!! never change the order !!!
        REDO_UNKNOWN,
        REDO_PARTITION_MIGRATING,
        REDO_MEMBER_UNKNOWN,
        REDO_TARGET_WRONG,
        REDO_TARGET_UNKNOWN,
        REDO_TARGET_DEAD,
        REDO_CONNECTION_NOT_ALIVE,
        REDO_PACKET_NOT_SENT,
        REDO_MAP_LOCKED,
        REDO_MAP_OVER_CAPACITY,
        REDO_QUEUE_NOT_MASTER,
        REDO_QUEUE_NOT_READY,
        ;

        public static RedoType getRedoType(int code) {
            final RedoType types[] = RedoType.values();
            if (code < 0 || code >= types.length) {
                return REDO_UNKNOWN;
            }
            return types[code];
        }

        public byte getCode() {
            return (byte) ordinal();
        }
    }
}
