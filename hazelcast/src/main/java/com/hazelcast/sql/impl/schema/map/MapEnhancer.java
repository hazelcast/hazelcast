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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;

/**
 * Plugin mechanism, allowing to perform additional analysis of
 * {@link IMap}/{@link ReplicatedMap} keys & values on sample based schema resolution.
 * <p/>
 * Used by Jet.
 */
@FunctionalInterface
public interface MapEnhancer {

    /**
     * Return an appendix that is attached to the resolved {@link AbstractMapTable}.
     */
    Object analyze(Object object, boolean key);
}
