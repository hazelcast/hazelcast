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

package com.hazelcast.internal.serialization.impl;

public final class SerializationConstants {

    // WARNING: DON'T CHANGE VALUES!
    // WARNING: DON'T ADD ANY NEW CONSTANT SERIALIZER!

    public static final int CONSTANT_TYPE_NULL = 0;

    public static final int CONSTANT_TYPE_PORTABLE = -1;

    public static final int CONSTANT_TYPE_DATA_SERIALIZABLE = -2;

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

    public static final int CONSTANT_TYPE_BOOLEAN_ARRAY = -13;

    public static final int CONSTANT_TYPE_CHAR_ARRAY = -14;

    public static final int CONSTANT_TYPE_SHORT_ARRAY = -15;

    public static final int CONSTANT_TYPE_INTEGER_ARRAY = -16;

    public static final int CONSTANT_TYPE_LONG_ARRAY = -17;

    public static final int CONSTANT_TYPE_FLOAT_ARRAY = -18;

    public static final int CONSTANT_TYPE_DOUBLE_ARRAY = -19;

    public static final int CONSTANT_TYPE_STRING_ARRAY = -20;

    // ------------------------------------------------------------
    // DEFAULT SERIALIZERS

    public static final int JAVA_DEFAULT_TYPE_CLASS = -21;

    public static final int JAVA_DEFAULT_TYPE_DATE = -22;

    public static final int JAVA_DEFAULT_TYPE_BIG_INTEGER = -23;

    public static final int JAVA_DEFAULT_TYPE_BIG_DECIMAL = -24;

    public static final int JAVA_DEFAULT_TYPE_ENUM = -25;

    public static final int JAVA_DEFAULT_TYPE_ARRAY_LIST = -26;

    public static final int JAVA_DEFAULT_TYPE_LINKED_LIST = -27;

    // NUMBER OF CONSTANT SERIALIZERS...
    public static final int CONSTANT_SERIALIZERS_LENGTH = 28;

    // ------------------------------------------------------------
    // JAVA SERIALIZATION

    public static final int JAVA_DEFAULT_TYPE_SERIALIZABLE = -100;
    public static final int JAVA_DEFAULT_TYPE_EXTERNALIZABLE = -101;

    // ------------------------------------------------------------
    // LANGUAGE SPECIFIC SERIALIZERS
    // USED BY CLIENTS (Not deserialized by server)

    public static final int CSHARP_CLR_SERIALIZATION_TYPE = -110;
    public static final int PYTHON_PICKLE_SERIALIZATION_TYPE = -120;
    public static final int JAVASCRIPT_JSON_SERIALIZATION_TYPE = -130;
    public static final int GO_GOB_SERIALIZATION_TYPE = -140;

    // ------------------------------------------------------------
    // HIBERNATE SERIALIZERS

    public static final int HIBERNATE3_TYPE_HIBERNATE_CACHE_KEY = -200;
    public static final int HIBERNATE3_TYPE_HIBERNATE_CACHE_ENTRY = -201;

    public static final int HIBERNATE4_TYPE_HIBERNATE_CACHE_KEY = -202;
    public static final int HIBERNATE4_TYPE_HIBERNATE_CACHE_ENTRY = -203;

    public static final int HIBERNATE5_TYPE_HIBERNATE_CACHE_KEY = -204;
    public static final int HIBERNATE5_TYPE_HIBERNATE_CACHE_ENTRY = -205;
    public static final int HIBERNATE5_TYPE_HIBERNATE_NATURAL_ID_KEY = -206;

    //--------------------------------------------------------------
    // RESERVED FOR JET -300 to -400

    public static final int JET_SERIALIZER_FIRST = -300;
    public static final int JET_SERIALIZER_LAST = -399;

    private SerializationConstants() {
    }
}
