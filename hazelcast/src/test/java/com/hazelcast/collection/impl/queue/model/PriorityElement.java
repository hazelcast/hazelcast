package com.hazelcast.collection.impl.queue.model;

import java.io.Serializable;

public class PriorityElement implements Serializable, Comparable<PriorityElement> {

	private static final long serialVersionUID = 1L;

	private Boolean highPriority;
	private Integer version;

	public PriorityElement(boolean highPriority, Integer version) {
		this.highPriority = highPriority;
		this.version = version;
	}

	public Boolean isHighPriority() {
		return this.highPriority;
	}

	public void setHighPriority(final Boolean highPriority) {
		this.highPriority = highPriority;
	}

	public Integer getVersion() {
		return version;
	}

	public void setVersion(Integer version) {
		this.version = version;
	}

	@Override
	public int compareTo(final PriorityElement other) {
		if (isHighPriority() && !other.isHighPriority()) {
			return -1;
		}
		if (other.isHighPriority() && !isHighPriority()) {
			return 1;
		}
		return getVersion().compareTo(other.getVersion());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((highPriority == null) ? 0 : highPriority.hashCode());
		result = prime * result + ((version == null) ? 0 : version.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		PriorityElement other = (PriorityElement) obj;
		if (highPriority == null) {
			if (other.highPriority != null) {
				return false;
			}
		} else if (!highPriority.equals(other.highPriority)) {
			return false;
		}
		if (version == null) {
			if (other.highPriority != null) {
				return false;
			}
		} else if (!highPriority.equals(other.highPriority)) {
			return false;
		}
		return true;
	}
}
