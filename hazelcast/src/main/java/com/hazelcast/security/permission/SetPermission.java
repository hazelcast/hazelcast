package com.hazelcast.security.permission;


public class SetPermission extends ListPermission {

	public SetPermission(String name, String... actions) {
		super(name, actions);
	}
	
}
