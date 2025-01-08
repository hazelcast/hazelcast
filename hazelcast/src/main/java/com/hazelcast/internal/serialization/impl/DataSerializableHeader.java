/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;


/**
 * The serializaion header consists of one java byte primitive value.
 * Bits are used in the following way (from the least significant to the most significant)
 * 0.) 0=data_serializable, 1=identified_data_serializable
 * 1.) 0=non-versioned, 1=versioned
 * 2.) unused
 * 3.) unused
 * 4.) unused
 * 5.) unused
 * 6.) unused
 * 7.) unused
 * <p>
 * Earlier the header was just a byte holding boolean value:
 * - 0=data_serializable, 1=identified_data_serializable
 * thus the new format is fully backward compatible.
 */
final class DataSerializableHeader {

    public static final int FACTORY_AND_CLASS_ID_BYTE_LENGTH = 8;
    public static final int EE_BYTE_LENGTH = 2;

    private static final byte IDENTIFIED_DATA_SERIALIZABLE = 1 << 0;
    private static final byte VERSIONED = 1 << 1;

    private DataSerializableHeader() {
    }

    static boolean isIdentifiedDataSerializable(byte header) {
        return (header & IDENTIFIED_DATA_SERIALIZABLE) != 0;
    }

    static boolean isVersioned(byte header) {
        return (header & VERSIONED) != 0;
    }

    static byte createHeader(boolean identified, boolean versioned) {
        byte header = 0;

        if (identified) {
            header |= IDENTIFIED_DATA_SERIALIZABLE;
        }
        if (versioned) {
            header |= VERSIONED;
        }

        return header;
    }

}
