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

import java.util.Set;

/**
 * Defines a class schema for {@link Portable} classes. It allows querying of field names, types, class IDs etc.
 * It can be created manually using {@link ClassDefinitionBuilder} or on demand during deserialization.
 *
 * @see com.hazelcast.nio.serialization.Portable
 * @see ClassDefinitionBuilder
 */
public interface ClassDefinition {

    /**
     * Gets the FieldDefinition for a particular field.
     *
     * @param name name of the field
     * @return field definition by given name or null
     */
    FieldDefinition getField(String name);

    /**
     * Gets the FieldDefinition for a given fieldIndex.
     *
     * @param fieldIndex index of the field
     * @return field definition by given index
     * @throws java.lang.IndexOutOfBoundsException if the fieldIndex is invalid.
     */
    FieldDefinition getField(int fieldIndex);

    /**
     * Checks if there exists a FieldDefinition with the given fieldName.
     *
     * @param fieldName field name
     * @return true if this class definition contains a field named by given name
     */
    boolean hasField(String fieldName);

    /**
     * Returns a Set of all field names.
     *
     * @return all field names contained in this class definition
     */
    Set<String> getFieldNames();

    /**
     * Get the FieldType for a given fieldName.
     *
     * @param fieldName name of the field
     * @return type of given field
     * @throws java.lang.IllegalArgumentException if the field does not exist.
     */
    FieldType getFieldType(String fieldName);

    /**
     * Gets the class ID of a field.
     *
     * @param fieldName name of the field
     * @return class ID of given field
     * @throws java.lang.IllegalArgumentException if the field does not not exist
     */
    int getFieldClassId(String fieldName);

    /**
     * Returns the field count.
     *
     * @return total field count
     */
    int getFieldCount();

    /**
     * Returns the factory ID.
     *
     * @return factory ID
     */
    int getFactoryId();

    /**
     * Returns the class ID.
     *
     * @return class ID
     */
    int getClassId();

    /**
     * Returns the version.
     *
     * @return version
     */
    int getVersion();
}
