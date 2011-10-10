package com.hazelcast.config;

import java.util.HashSet;
import java.util.Set;

public class PermissionConfig {

	private PermissionType type;
	private String name;
	private String principal;
	private Set<String> endpoints ;
	private Set<String> actions ;
	
	public PermissionConfig() {
		super();
	}
	
	public PermissionConfig(PermissionType type, String name, String principal) {
		super();
		this.type = type;
		this.name = name;
		this.principal = principal;
	}
	
	public enum PermissionType {
		MAP, QUEUE, TOPIC, MULTIMAP, LIST, SET, 
		LOCK, ATOMIC_NUMBER, COUNTDOWN_LATCH, SEMAPHORE, 
		EXECUTOR_SERVICE, LISTENER, TRANSACTION, ALL
	}
	
	public void addEndpoint(String endpoint) {
		if(endpoints == null) {
			endpoints = new HashSet<String>();
		}
		endpoints.add(endpoint);
	}
	
	public void addAction(String action) {
		if(actions == null) {
			actions = new HashSet<String>();
		}
		actions.add(action);
	}

	public PermissionType getType() {
		return type;
	}

	public String getName() {
		return name;
	}

	public String getPrincipal() {
		return principal;
	}
	
	public Set<String> getEndpoints() {
		if(endpoints == null) {
			endpoints = new HashSet<String>();
		}
		return endpoints;
	}

	public Set<String> getActions() {
		if(actions == null) {
			actions = new HashSet<String>();
		}
		return actions;
	}

	public void setType(PermissionType type) {
		this.type = type;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setPrincipal(String principal) {
		this.principal = principal;
	}

	public void setEndpoint(String endpoint) {
		addEndpoint(endpoint);
	}

	public void setActions(Set<String> actions) {
		this.actions = actions;
	}
}
