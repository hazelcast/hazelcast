package com.hazelcast.collection.impl.queue.duplicate;

import java.io.Serializable;

public interface PriorityQueue<T extends Serializable> {

  boolean enqueue(final T value);

  T dequeue();
}
