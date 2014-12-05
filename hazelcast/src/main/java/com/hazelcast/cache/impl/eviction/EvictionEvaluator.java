/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.eviction;

/**
 * An {@link EvictionEvaluator} is used to implement logic to decide if an eviction should be
 * executed or not such as based on the size of an {EvictableStore}.
 *
 * @param <A> accessor (key) type of the evictable entry
 * @param <E> {@link com.hazelcast.cache.impl.eviction.Evictable} type (value) of the entry
 * @param <S> type of the {@link com.hazelcast.cache.impl.eviction.EvictableStore} that stores the entries
 */
public interface EvictionEvaluator<A, E extends Evictable, S extends EvictableStore<A, E>> {

    /**
     * The isEvictionRequired method implementation will decide, based on the passed
     * {@link EvictableStore} if an eviction is necessary or not.
     *
     * @return <tt>true</tt> if eviction is required, otherwise <tt>false</tt>
     */
    boolean isEvictionRequired(S evictableStore);

}
