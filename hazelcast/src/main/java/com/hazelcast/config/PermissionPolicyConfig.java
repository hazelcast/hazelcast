package com.hazelcast.config;

import java.util.Properties;

import com.hazelcast.security.IPermissionPolicy;

public class PermissionPolicyConfig {
	
	private String className = null;
	
	private IPermissionPolicy policyImpl = null;
	
	private Properties properties = new Properties();

	public PermissionPolicyConfig() {
		super();
	}
	
	public PermissionPolicyConfig(String className) {
		super();
		this.className = className;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public IPermissionPolicy getPolicyImpl() {
		return policyImpl;
	}

	public void setPolicyImpl(IPermissionPolicy policyImpl) {
		this.policyImpl = policyImpl;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	
}
