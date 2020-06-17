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

import com.hazelcast.internal.serialization.SerializationService;

import java.util.Comparator;
import java.util.Objects;

/**
 * A comparator which forwards its {@link Comparator#compare(Object, Object)} to another supplied comparator.
 * If {@code comparator} is {@code null}, then {@link Comparator#compare(Object, Object)}
 * uses {@link QueueItem#compareTo(QueueItem)}
 *
 * @param <T> the type of objects that may be compared by the delegated comparator
 *           as defined by the supplied comparator instance in {QueueConfig}
 */
public final class ForwardingQueueItemComparator<T> implements Comparator<QueueItem> {

    private final Comparator<T> comparator;
    private final SerializationService serializationService;

    public ForwardingQueueItemComparator(final Comparator<T> comparator,
                                         final SerializationService serializationService) {
        this.comparator = comparator;
        this.serializationService = serializationService;
    }

    @Override
    public int compare(QueueItem o1, QueueItem o2) {
        if (comparator == null) {
            return o1.compareTo(o2);
        }
        T object1 = (T) serializationService.toObject(o1.getData());
        T object2 = (T) serializationService.toObject(o2.getData());

        return this.comparator.compare(object1, object2);
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
        return Objects.equals(comparator, that.comparator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(comparator);
    }
}
