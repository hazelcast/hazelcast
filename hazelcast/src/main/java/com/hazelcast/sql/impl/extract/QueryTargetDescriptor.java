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

package com.hazelcast.sql.impl.extract;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.getters.Extractors;

/**
 * Descriptor of the target object (key or value from map record). Used to generate the proper attribute extraction strategy
 * and to impose additional restrictions on data that is used for compilation. For example, expected class name or expected
 * portable class ID.
 * <p>
 * The principal difference to {@link QueryTarget} is that descriptor is transferred over a wire, while the target object
 * may contain non-serializable internals.
 */
public interface QueryTargetDescriptor extends DataSerializable {
    /**
     * Create the target that will be used for extraction.
     *
     * @param serializationService Serialization service.
     * @param extractors Extractors associated with the map.
     * @param isKey Whether this target extract data from key or value. Used mainly for proper error reporting.
     * @return Target object.
     */
    QueryTarget create(InternalSerializationService serializationService, Extractors extractors, boolean isKey);
}
