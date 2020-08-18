package com.hazelcast.collection.impl.queue.duplicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.impl.queue.model.PriorityElement;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastTestSupport;

public class PriorityElementTaskQueueImpl extends HazelcastTestSupport implements PriorityElementTaskQueue {

  public static final String PRIORITY_ELEMENT_QUEUE_NAME = "PRIORITY_ELEMENT_QUEUE";
  public static final String PRIORITY_ELEMENT_MAP_NAME = "PRIORITY_ELEMENT_MAP";

  private final static Logger LOG = LoggerFactory.getLogger(PriorityElementTaskQueueImpl.class);

  private final IQueue<PriorityElement> queue;

  private final IMap<PriorityElement, PriorityElement> map;

  public PriorityElementTaskQueueImpl() {
    
	Config config = new Config();
	config.getQueueConfig(PRIORITY_ELEMENT_QUEUE_NAME)
			.setPriorityComparatorClassName("com.hazelcast.collection.impl.queue.model.PriorityElementComparator");
	HazelcastInstance hz = createHazelcastInstance(config);
	queue = hz.getQueue(PRIORITY_ELEMENT_QUEUE_NAME);
	map = hz.getMap(PRIORITY_ELEMENT_MAP_NAME);

  }

  @Override
  public boolean enqueue(final PriorityElement fahrplanTask) {
    try {
    	PriorityElement previousValue = map.get(fahrplanTask);

      if (previousValue != null) {
        return false;
      }

      boolean added = queue.offer(fahrplanTask);
      if (added) {
        map.put(fahrplanTask, fahrplanTask);
      }
      return added;
    }
    catch (Exception e) {
      LOG.warn("Unable to write to priorityQueue: " + e);
      return false;
    }

  }

  @Override
  public PriorityElement dequeue() {
    try {
      final PriorityElement element = queue.poll();
      if (element != null) {
        map.remove(element);
      }
      return element;
    }
    catch (Exception e) {
      LOG.warn("Unable to read from priorityQueue: " + e);
      return null;
    }
  }

  @Override
  public void clear() {
    try {
      queue.clear();
      map.clear();
    }
    catch (Exception e) {
      LOG.warn("Unable to clear priorityQueue", e);
    }
  }
}