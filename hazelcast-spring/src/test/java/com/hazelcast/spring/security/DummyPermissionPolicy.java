package com.hazelcast.spring.security;

import java.security.Permission;
import java.security.PermissionCollection;
import java.util.Properties;

import javax.security.auth.Subject;

import com.hazelcast.config.SecurityConfig;
import com.hazelcast.security.IPermissionPolicy;

public class DummyPermissionPolicy implements IPermissionPolicy {

	public void configure(SecurityConfig securityConfig, Properties properties) {
	}

	public PermissionCollection getPermissions(Subject subject,
			Class<? extends Permission> type) {
		return null;
	}

	public void destroy() {
	}

}
