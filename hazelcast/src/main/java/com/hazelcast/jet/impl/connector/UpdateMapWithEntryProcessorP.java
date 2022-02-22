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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;

public final class UpdateMapWithEntryProcessorP<T, K, V, R> extends AsyncHazelcastWriterP {

    private final IMap<K, V> map;
    private final FunctionEx<? super T, ? extends K> toKeyFn;
    private final FunctionEx<? super T, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn;

    public UpdateMapWithEntryProcessorP(
        @Nonnull HazelcastInstance instance,
        int maxParallelAsyncOps,
        @Nonnull String name,
        @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
        @Nonnull FunctionEx<? super T, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn
    ) {
        super(instance, maxParallelAsyncOps);
        this.map = instance.getMap(name);
        this.toKeyFn = toKeyFn;
        this.toEntryProcessorFn = toEntryProcessorFn;
    }

    @Override
    protected void processInternal(Inbox inbox) {
        int permits = tryAcquirePermits(inbox.size());
        for (Object object; permits > 0 && (object = inbox.peek()) != null; permits--) {
            @SuppressWarnings("unchecked")
            T item = (T) object;
            EntryProcessor<K, V, R> entryProcessor = toEntryProcessorFn.apply(item);
            K key = toKeyFn.apply(item);
            setCallback(map.submitToKey(key, entryProcessor));
            inbox.remove();
        }
    }
}
