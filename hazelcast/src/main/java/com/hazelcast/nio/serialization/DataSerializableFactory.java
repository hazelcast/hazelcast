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
 * DataSerializableFactory is used to create IdentifiedDataSerializable instances during de-serialization.
 *
 * @see com.hazelcast.nio.serialization.IdentifiedDataSerializable
 */
public interface DataSerializableFactory {

    /**
     * Creates an IdentifiedDataSerializable instance using given type id
     * @param typeId IdentifiedDataSerializable type id
     * @return IdentifiedDataSerializable instance or null if type id is not known by this factory
     */
    IdentifiedDataSerializable create(int typeId);

}
