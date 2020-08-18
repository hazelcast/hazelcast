package com.hazelcast.collection.impl.queue.model;

import java.io.Serializable;
import java.util.Comparator;

public class PriorityElementComparator implements Comparator<PriorityElement>, Serializable {
	
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(PriorityElement o1, PriorityElement o2) {
		if (o1.isHighPriority() && !o2.isHighPriority()) {
			return -1;
		}
		if (o2.isHighPriority() && !o1.isHighPriority()) {
			return 1;
		}
		return o1.getVersion().compareTo(o2.getVersion());
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
