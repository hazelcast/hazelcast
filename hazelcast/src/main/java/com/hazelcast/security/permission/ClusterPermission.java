package com.hazelcast.security.permission;

import java.security.Permission;
import java.security.PermissionCollection;

public abstract class ClusterPermission extends Permission {
	
	private int hashcode = 0;

	public ClusterPermission(String name) {
		super(name);
	}
	
	public PermissionCollection newPermissionCollection() {
		return new ClusterPermissionCollection(getClass());
	}
	
	public int hashCode() {
		if(hashcode == 0) {
			final int prime = 31;
			int result = 1;
			result = prime * result + (getName() != null 
					? getName().hashCode() : 13);
			hashcode = result;
		}
		return hashcode;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ClusterPermission other = (ClusterPermission) obj;
		if(getName() == null && other.getName() != null){
			return false;
		}
		if(!getName().equals(other.getName())) {
			return false;
		}
		return true;
	}
}
