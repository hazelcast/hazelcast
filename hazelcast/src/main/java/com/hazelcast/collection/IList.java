/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection;

import com.hazelcast.config.SplitBrainProtectionConfig;

import java.util.List;

/**
 * Concurrent, distributed implementation of {@link List}.
 *
 * <p>The Hazelcast IList is not a partitioned data-structure. Entire contents
 * of an IList is stored on a single machine (and in the backup). The IList
 * will not scale by adding more members to the cluster.
 *
 * <p>Supports split brain protection {@link SplitBrainProtectionConfig} since 3.10
 * in cluster versions 3.10 and higher.
 *
 * @param <E>
 * @see List
 */
public interface IList<E> extends List<E>, ICollection<E> {
}
