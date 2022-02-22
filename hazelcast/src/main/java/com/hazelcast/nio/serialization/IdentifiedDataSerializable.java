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
 * IdentifiedDataSerializable is an extension to {@link com.hazelcast.nio.serialization.DataSerializable}
 * to avoid reflection during de-serialization.
 *
 * Each IdentifiedDataSerializable is
 * created by a registered {@link com.hazelcast.nio.serialization.DataSerializableFactory}.
 *
 * @see com.hazelcast.nio.serialization.DataSerializable
 * @see com.hazelcast.nio.serialization.Portable
 * @see com.hazelcast.nio.serialization.DataSerializableFactory
 */
public interface IdentifiedDataSerializable extends DataSerializable {

    /**
     * Returns DataSerializableFactory factory ID for this class.
     *
     * @return factory ID
     */
    int getFactoryId();

    /**
     * Returns type identifier for this class. It should be unique per DataSerializableFactory.
     *
     * @return type ID
     */
    int getClassId();
}
