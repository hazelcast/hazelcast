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

package com.hazelcast.collection.impl.queue;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.hazelcast.internal.serialization.SerializationService;
import java.util.Comparator;
import java.util.Objects;

/**
 * A comparator which forwards its {@link Comparator#compare(Object, Object)} to another supplied comparator.
 *
 * @param <T> the type of objects that may be compared by the delegated comparator
 *           as defined by the supplied comparator instance in {QueueConfig}
 */
public final class ForwardingQueueItemComparator<T> implements Comparator<QueueItem> {

    private static final int DEFAULT_CACHE_SIZE = 1000000;
    private final Comparator<T> customComparator;
    private final LoadingCache<QueueItem, T> deserializedDataCache;
    private final SerializationService serializationService;
    private final CacheLoader<QueueItem, T> loader = new CacheLoader<QueueItem, T>() {

        @Override
        public T load(QueueItem queueItem) {
            return serializationService.toObject(queueItem.getData());
        }
    };

    public ForwardingQueueItemComparator(final Comparator<T> customComparator,
            final SerializationService serializationService) {
        Objects.requireNonNull(customComparator, "Custom comparator cannot be null.");
        Objects.requireNonNull(serializationService, "SerializationService cannot be null.");
        this.customComparator = customComparator;
        this.serializationService = serializationService;
        this.deserializedDataCache = CacheBuilder.newBuilder().maximumSize(DEFAULT_CACHE_SIZE).build(loader);
    }

    @Override
    public int compare(QueueItem o1, QueueItem o2) {
        T object1 = deserializedDataCache.getUnchecked(o1);
        T object2 = deserializedDataCache.getUnchecked(o2);

        return this.customComparator.compare(object1, object2);
    }

    @Override
    public String toString() {
        return "ForwardingQueueItemComparator{" + super.toString() + "} ";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ForwardingQueueItemComparator<?> that = (ForwardingQueueItemComparator<?>) o;
        return Objects.equals(customComparator, that.customComparator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(customComparator);
    }
}
