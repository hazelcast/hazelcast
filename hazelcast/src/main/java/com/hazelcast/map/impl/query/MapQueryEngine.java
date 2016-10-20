/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.query.Predicate;
import com.hazelcast.util.IterationType;

import java.util.Set;

/**
 * Responsible for executing queries on the IMap.
 */
public interface MapQueryEngine {

    Set runQueryOnAllPartitions(String mapName, Predicate predicate, IterationType iterationType, boolean uniqueResult);

    Set runQueryOnLocalPartitions(String mapName, Predicate predicate, IterationType iterationType, boolean uniqueResult);

    Set runQueryOnGivenPartition(String mapName, Predicate predicate, IterationType iterationType, boolean uniqueResult,
                                 int partitionId);

}
