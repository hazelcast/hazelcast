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

package com.hazelcast.jet;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

/**
 * A distributed, in-memory concurrent map implementation.
 * <p>
 * The entries of the map are distributed across the whole cluster and partitioned
 * by key.
 * <p>
 * It's possible to use the map as a data source or sink in a Jet {@link Pipeline},
 * using {@link Sources#map(String)} or {@link Sinks#map(String)} and the
 * change stream of the map can be read using
 * {@link Sources#mapJournal(String, JournalInitialPosition)}.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of values
 *
 * @see IMap
 * @see Sources#map(String)
 * @see Sources#mapJournal(String, JournalInitialPosition)
 * @see Sinks#map(String) (String)
 */
public interface IMapJet<K, V> extends IMap<K, V> {

}
