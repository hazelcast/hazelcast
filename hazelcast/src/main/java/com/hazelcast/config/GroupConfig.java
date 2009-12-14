package com.hazelcast.config;

public class GroupConfig {

    public static final String DEFAULT_GROUP_PASSWORD = "dev-pass";
    public static final String DEFAULT_GROUP_NAME = "dev";

    private String name = DEFAULT_GROUP_NAME;
	private String password = DEFAULT_GROUP_PASSWORD;
	
	public GroupConfig() {
	}
	
	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public GroupConfig setName(String name) {
		this.name = name;
		return this;
	}
	
	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}
	
	/**
	 * @param password the password to set
	 */
	public GroupConfig setPassword(String password) {
		this.password = password;
		return this;
	}
	
}
