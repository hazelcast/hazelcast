package com.hazelcast.security.permission;

import java.security.Permission;
import java.security.PermissionCollection;
import java.util.Enumeration;

public class DenyAllPermissionCollection extends PermissionCollection {

	public DenyAllPermissionCollection() {
		super();
	}

	public void add(Permission permission) {
	}

	public boolean implies(Permission permission) {
		return false;
	}

	public Enumeration<Permission> elements() {
		return new Enumeration<Permission>() {
			public boolean hasMoreElements() {
				return false;
			}
			public Permission nextElement() {
				return null;
			}
		};
	}

	public int hashCode() {
		return 37;
	}

	public String toString() {
		return "<deny all permissions>";
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof DenyAllPermissionCollection;
	}
}
