package com.hazelcast.collection.impl.queue;

import com.hazelcast.internal.serialization.SerializationService;

import java.io.Serializable;
import java.util.Comparator;

/**
 * A comparator which forwards its {@link Comparator#compare(Object, Object)} to another supplied comparator.
 * If {@code comparator} is {@code null}, then {@link Comparator#compare(Object, Object)} uses {@link QueueItem#compareTo(QueueItem)}
 *
 * @param <T> the type of objects that may be compared by the delegated comparator as defined by the supplied comparator instance in {QueueConfig}
 */
public final class ForwardingQueueItemComparator<T> implements Comparator<QueueItem>, Serializable {

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
    public final boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        return getClass().equals(o.getClass());
    }

    @Override
    public final int hashCode() {
        return getClass().hashCode();
    }
}
