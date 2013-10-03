package com.hazelcast.security.permission;

import java.security.Permission;

public class ListenerPermission extends ClusterPermission {
	
	public ListenerPermission(String name) {
		super(name);
	}

	public boolean implies(Permission permission) {
		if(this.getClass() != permission.getClass()) {
			return false;
		}
		
		InstancePermission that = (InstancePermission) permission;
		if("all".equals(that.getName())
				|| that.getName().equals(this.getName())) {
			return true;
		}
		
		return false;
	}

	public String getActions() {
		return "listener";
	}
}
