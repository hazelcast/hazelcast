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

package com.hazelcast.nio.serialization;

public final class SerializationConstants {

    // WARNING: DON'T CHANGE VALUES!
    // WARNING: DON'T ADD ANY NEW CONSTANT SERIALIZER!

    public static final int CONSTANT_TYPE_PORTABLE = -1;

    public static final int CONSTANT_TYPE_DATA = -2;

    public static final int CONSTANT_TYPE_BYTE = -3;

    public static final int CONSTANT_TYPE_BOOLEAN = -4;

    public static final int CONSTANT_TYPE_CHAR = -5;

    public static final int CONSTANT_TYPE_SHORT = -6;

    public static final int CONSTANT_TYPE_INTEGER = -7;

    public static final int CONSTANT_TYPE_LONG = -8;

    public static final int CONSTANT_TYPE_FLOAT = -9;

    public static final int CONSTANT_TYPE_DOUBLE = -10;

    public static final int CONSTANT_TYPE_STRING = -11;

    public static final int CONSTANT_TYPE_BYTE_ARRAY = -12;

    public static final int CONSTANT_TYPE_CHAR_ARRAY = -13;

    public static final int CONSTANT_TYPE_SHORT_ARRAY = -14;

    public static final int CONSTANT_TYPE_INTEGER_ARRAY = -15;

    public static final int CONSTANT_TYPE_LONG_ARRAY = -16;

    public static final int CONSTANT_TYPE_FLOAT_ARRAY = -17;

    public static final int CONSTANT_TYPE_DOUBLE_ARRAY = -18;

    // NUMBER OF CONSTANT SERIALIZERS...
    public static final int CONSTANT_SERIALIZERS_LENGTH = 18;

    // ------------------------------------------------------------
    // DEFAULT SERIALIZERS

    public static final int DEFAULT_TYPE_CLASS = -19;

    public static final int DEFAULT_TYPE_DATE = -20;

    public static final int DEFAULT_TYPE_BIG_INTEGER = -21;

    public static final int DEFAULT_TYPE_BIG_DECIMAL = -22;

    public static final int DEFAULT_TYPE_OBJECT = -23;

    public static final int DEFAULT_TYPE_EXTERNALIZABLE = -24;

    public static final int DEFAULT_TYPE_ENUM = -25;

    // ------------------------------------------------------------
    // AUTOMATICALLY REGISTERED SERIALIZERS

    public static final int AUTO_TYPE_ARRAY_LIST = -100;

    public static final int AUTO_TYPE_JOB_PARTITION_STATE = -101;

    public static final int AUTO_TYPE_JOB_PARTITION_STATE_ARRAY = -102;

    public static final int AUTO_TYPE_LINKED_LIST = -103;

    public static final int AUTO_TYPE_HASH_MAP = -104;

    public static final int AUTO_TYPE_TREE_MAP = -105;

    // ------------------------------------------------------------
    // HIBERNATE SERIALIZERS

    public static final int HIBERNATE3_TYPE_HIBERNATE_CACHE_KEY = -200;
    public static final int HIBERNATE3_TYPE_HIBERNATE_CACHE_ENTRY = -201;

    public static final int HIBERNATE4_TYPE_HIBERNATE_CACHE_KEY = -202;
    public static final int HIBERNATE4_TYPE_HIBERNATE_CACHE_ENTRY = -203;

    private SerializationConstants() {
    }
}
