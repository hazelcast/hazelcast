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

package com.hazelcast.internal.compatibility.serialization.impl;

/**
 * Serialization constants for compatibility with compatibility (4.x) members
 */
public final class CompatibilitySerializationConstants {

    // WARNING: DON'T CHANGE VALUES!
    // WARNING: DON'T ADD ANY NEW CONSTANT SERIALIZER!

    public static final int CONSTANT_TYPE_UUID = -21;

    public static final int CONSTANT_TYPE_SIMPLE_ENTRY = -22;

    public static final int CONSTANT_TYPE_SIMPLE_IMMUTABLE_ENTRY = -23;

    // ------------------------------------------------------------
    // DEFAULT SERIALIZERS

    public static final int JAVA_DEFAULT_TYPE_CLASS = -24;

    public static final int JAVA_DEFAULT_TYPE_DATE = -25;

    public static final int JAVA_DEFAULT_TYPE_BIG_INTEGER = -26;

    public static final int JAVA_DEFAULT_TYPE_BIG_DECIMAL = -27;

    public static final int JAVA_DEFAULT_TYPE_ARRAY = -28;

    public static final int JAVA_DEFAULT_TYPE_ARRAY_LIST = -29;

    public static final int JAVA_DEFAULT_TYPE_LINKED_LIST = -30;

    public static final int JAVA_DEFAULT_TYPE_COPY_ON_WRITE_ARRAY_LIST = -31;


    public static final int JAVA_DEFAULT_TYPE_HASH_MAP = -32;

    public static final int JAVA_DEFAULT_TYPE_CONCURRENT_SKIP_LIST_MAP = -33;

    public static final int JAVA_DEFAULT_TYPE_CONCURRENT_HASH_MAP = -34;

    public static final int JAVA_DEFAULT_TYPE_LINKED_HASH_MAP = -35;

    public static final int JAVA_DEFAULT_TYPE_TREE_MAP = -36;


    public static final int JAVA_DEFAULT_TYPE_HASH_SET = -37;

    public static final int JAVA_DEFAULT_TYPE_TREE_SET = -38;

    public static final int JAVA_DEFAULT_TYPE_LINKED_HASH_SET = -39;

    public static final int JAVA_DEFAULT_TYPE_COPY_ON_WRITE_ARRAY_SET = -40;

    public static final int JAVA_DEFAULT_TYPE_CONCURRENT_SKIP_LIST_SET = -41;


    public static final int JAVA_DEFAULT_TYPE_ARRAY_DEQUE = -42;

    public static final int JAVA_DEFAULT_TYPE_LINKED_BLOCKING_QUEUE = -43;

    public static final int JAVA_DEFAULT_TYPE_ARRAY_BLOCKING_QUEUE = -44;

    public static final int JAVA_DEFAULT_TYPE_PRIORITY_BLOCKING_QUEUE = -45;

    public static final int JAVA_DEFAULT_TYPE_DELAY_QUEUE = -46;

    public static final int JAVA_DEFAULT_TYPE_SYNCHRONOUS_QUEUE = -47;

    public static final int JAVA_DEFAULT_TYPE_LINKED_TRANSFER_QUEUE = -48;

    public static final int JAVA_DEFAULT_TYPE_PRIORITY_QUEUE = -49;

    // NUMBER OF CONSTANT SERIALIZERS...
    public static final int CONSTANT_SERIALIZERS_LENGTH = 50;

    private CompatibilitySerializationConstants() {
    }
}
