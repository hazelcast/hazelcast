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

package com.hazelcast.nio.serialization;

/**
 * Defines the name, type and index of a field.
 */
public interface FieldDefinition {

    /**
     * @return field type
     */
    FieldType getType();

    /**
     * @return field name
     */
    String getName();

    /**
     * @return field index
     */
    int getIndex();

    /**
     * @return class ID of this field's class
     */
    int getClassId();

    /**
     * @return factory ID of this field's class
     */
    int getFactoryId();

    /**
     * @return version of this field's class
     */
    int getVersion();
}
