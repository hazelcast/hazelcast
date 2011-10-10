package com.hazelcast.config;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SecurityConfig {
	
	private boolean enabled = false;
	
	private CredentialsFactoryConfig memberCredentialsConfig = new CredentialsFactoryConfig();
	
	private List<LoginModuleConfig> memberLoginModuleConfigs = new ArrayList<LoginModuleConfig>();
	
	private List<LoginModuleConfig> clientLoginModuleConfigs = new ArrayList<LoginModuleConfig>();
	
	private PermissionPolicyConfig clientPolicyConfig = new PermissionPolicyConfig();
	
	private Set<PermissionConfig> clientPermissionConfigs = new HashSet<PermissionConfig>();
	
	public boolean isEnabled() {
		return enabled;
	}
	
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}
	
	public void addMemberLoginModuleConfig(LoginModuleConfig loginModuleConfig) {
		memberLoginModuleConfigs.add(loginModuleConfig);
	}
	
	public void addClientLoginModuleConfig(LoginModuleConfig loginModuleConfig) {
		clientLoginModuleConfigs.add(loginModuleConfig);
	}

	public void addClientPermissionConfig(PermissionConfig permissionConfig) {
		clientPermissionConfigs.add(permissionConfig);
	}

	public List<LoginModuleConfig> getClientLoginModuleConfigs() {
		return clientLoginModuleConfigs;
	}

	public void setClientLoginModuleConfigs(List<LoginModuleConfig> loginModuleConfigs) {
		this.clientLoginModuleConfigs = loginModuleConfigs;
	}
	
	public List<LoginModuleConfig> getMemberLoginModuleConfigs() {
		return memberLoginModuleConfigs;
	}
	
	public void setMemberLoginModuleConfigs(
			List<LoginModuleConfig> memberLoginModuleConfigs) {
		this.memberLoginModuleConfigs = memberLoginModuleConfigs;
	}
	
	public PermissionPolicyConfig getClientPolicyConfig() {
		return clientPolicyConfig;
	}

	public void setClientPolicyConfig(PermissionPolicyConfig policyConfig) {
		this.clientPolicyConfig = policyConfig;
	}

	public Set<PermissionConfig> getClientPermissionConfigs() {
		return clientPermissionConfigs;
	}

	public void setClientPermissionConfigs(Set<PermissionConfig> permissions) {
		this.clientPermissionConfigs = permissions;
	}
	
	public CredentialsFactoryConfig getMemberCredentialsConfig() {
		return memberCredentialsConfig;
	}
	
	public void setMemberCredentialsConfig(
			CredentialsFactoryConfig credentialsFactoryConfig) {
		this.memberCredentialsConfig = credentialsFactoryConfig;
	}
}
