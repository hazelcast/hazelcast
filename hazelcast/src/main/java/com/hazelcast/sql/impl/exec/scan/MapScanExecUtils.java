/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec.scan;

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.query.impl.getters.Extractors;

/**
 * Utility class containing helper methods for map iteration. Simplifies implementation of query compiler.
 */
public final class MapScanExecUtils {
    private MapScanExecUtils() {
        // No-op.
    }

    public static Extractors createExtractors(MapContainer map) {
        return map.getExtractors();
    }

    public static MapScanExecIterator createIterator(MapContainer map, PartitionIdSet parts) {
        return new MapScanExecIterator(map, parts.iterator());
    }
}
