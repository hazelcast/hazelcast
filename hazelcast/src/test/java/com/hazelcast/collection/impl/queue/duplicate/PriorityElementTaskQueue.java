package com.hazelcast.collection.impl.queue.duplicate;

import com.hazelcast.collection.impl.queue.model.PriorityElement;

public interface PriorityElementTaskQueue extends PriorityQueue<PriorityElement> {

  void clear();
}
