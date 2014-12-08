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

import java.util.Set;

/**
 * ClassDefinition defines a class schema for Portable classes. It allows to query field names, types, class id etc.
 * It can be created manually using {@link com.hazelcast.nio.serialization.ClassDefinitionBuilder}
 * or ondemand during serialization phase.
 *
 * @see com.hazelcast.nio.serialization.Portable
 * @see com.hazelcast.nio.serialization.ClassDefinitionBuilder
 */
public interface ClassDefinition {

    /**
     * @param name name of the field
     * @return field definition by given name or null
     */
    FieldDefinition getField(String name);

    /**
     * @param fieldIndex index of the field
     * @return field definition by given index
     * @throws java.lang.IndexOutOfBoundsException
     */
    FieldDefinition getField(int fieldIndex);

    /**
     * @param fieldName field name
     * @return true if this class definition contains a field named by given name
     */
    boolean hasField(String fieldName);

    /**
     * @return all field names contained in this class definition
     */
    Set<String> getFieldNames();

    /**
     * @param fieldName name of the field
     * @return type of given field
     * @throws java.lang.IllegalArgumentException
     */
    FieldType getFieldType(String fieldName);

    /**
     * @param fieldName name of the field
     * @return class id of given field
     * @throws java.lang.IllegalArgumentException
     */
    int getFieldClassId(String fieldName);

    /**
     * @return total field count
     */
    int getFieldCount();

    /**
     * @return factory id
     */
    int getFactoryId();

    /**
     * @return class id
     */
    int getClassId();

    /**
     * @return version
     */
    int getVersion();

}
