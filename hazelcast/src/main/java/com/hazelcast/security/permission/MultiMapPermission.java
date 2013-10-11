package com.hazelcast.security.permission;

public class MultiMapPermission extends MapPermission {
	
	public MultiMapPermission(String name, String... actions) {
		super(name, actions);
	}
}
