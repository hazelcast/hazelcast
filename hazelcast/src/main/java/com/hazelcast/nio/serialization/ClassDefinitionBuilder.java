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

/**
 * @mdogan 2/6/13
 */
public interface ClassDefinitionBuilder {

    void addIntField(String fieldName);

    void addLongField(String fieldName);

    void addUTFField(String fieldName);

    void addBooleanField(String fieldName);

    void addByteField(String fieldName);

    void addCharField(String fieldName);

    void addDoubleField(String fieldName);

    void addFloatField(String fieldName);

    void addShortField(String fieldName);

    void addByteArrayField(String fieldName);

    void addCharArrayField(String fieldName);

    void addIntArrayField(String fieldName);

    void addLongArrayField(String fieldName);

    void addDoubleArrayField(String fieldName);

    void addFloatArrayField(String fieldName);

    void addShortArrayField(String fieldName);

    ClassDefinitionBuilder createPortableFieldBuilder(String fieldName, int classId);

    ClassDefinitionBuilder createPortableArrayFieldBuilder(String fieldName, int classId);

    void buildAndRegister();

}
