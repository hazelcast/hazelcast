/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

/**
 * @mdogan 1/1/13
 */
public interface FieldDefinition extends DataSerializable {

    final byte TYPE_PORTABLE = 0;
    final byte TYPE_BYTE = 1;
    final byte TYPE_BOOLEAN = 2;
    final byte TYPE_CHAR = 3;
    final byte TYPE_SHORT = 4;
    final byte TYPE_INT = 5;
    final byte TYPE_LONG = 6;
    final byte TYPE_FLOAT = 7;
    final byte TYPE_DOUBLE = 8;
    final byte TYPE_UTF = 9;
    final byte TYPE_PORTABLE_ARRAY = 10;
    final byte TYPE_BYTE_ARRAY = 11;
    final byte TYPE_CHAR_ARRAY = 12;
    final byte TYPE_SHORT_ARRAY = 13;
    final byte TYPE_INT_ARRAY = 14;
    final byte TYPE_LONG_ARRAY = 15;
    final byte TYPE_FLOAT_ARRAY = 16;
    final byte TYPE_DOUBLE_ARRAY = 17;

    byte getType();

    String getName();

    int getIndex();

    int getClassId();
}
