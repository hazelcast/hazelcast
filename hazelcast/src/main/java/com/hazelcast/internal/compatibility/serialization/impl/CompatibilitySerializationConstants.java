/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
 * Serialization constants for compatibility with compatibility (3.x) members
 */
public final class CompatibilitySerializationConstants {

    // WARNING: DON'T CHANGE VALUES!
    // WARNING: DON'T ADD ANY NEW CONSTANT SERIALIZER!

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

    private CompatibilitySerializationConstants() {
    }
}
