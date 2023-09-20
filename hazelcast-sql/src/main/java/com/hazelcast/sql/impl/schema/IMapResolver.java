/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.schema;

import javax.annotation.Nullable;

/**
 * Interface that creates {@link Mapping} objects based on IMap name by
 * sampling the map data. If the IMap does not exist or is empty, the mapping
 * cannot be generated because types cannot be inferred.
 */
@FunctionalInterface
public interface IMapResolver {

    @Nullable
    Mapping resolve(String iMapName);
}
