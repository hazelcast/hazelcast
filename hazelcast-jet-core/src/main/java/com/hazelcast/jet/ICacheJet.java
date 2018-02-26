/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.ICache;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.util.concurrent.ConcurrentMap;

/**
 * A distributed, in-memory implementation of {@link javax.cache.Cache JCache}
 * specification.
 * <p>
 * The entries of the cache are distributed across the whole cluster and partitioned
 * by key.
 * <p>
 * Main difference with {@link IMapJet} is that the API implements JCache
 * specification rather than {@link ConcurrentMap}, otherwise both
 * implementations have a lot of similarities.
 * <p>
 * As with {@link IMapJet}, it's possible to use the cache as a data source
 * or sink in a Jet {@link Pipeline}, using {@link Sources#cache(String)}
 * or {@link Sinks#cache(String)} and the change stream of the cache
 * can be read using {@link Sources#cacheJournal(String, JournalInitialPosition)}.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of values
 *
 * @see ICache
 * @see Sources#cache(String)
 * @see Sources#cacheJournal(String, JournalInitialPosition)
 * @see Sinks#cache(String) (String)
 */
public interface ICacheJet<K, V> extends ICache<K, V> {
}
