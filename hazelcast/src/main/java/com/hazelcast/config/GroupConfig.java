package com.hazelcast.config;

public class GroupConfig {

    public static final String DEFAULT_GROUP_PASSWORD = "dev-pass";
    public static final String DEFAULT_GROUP_NAME = "dev";

    private String name = DEFAULT_GROUP_NAME;
	private String password = DEFAULT_GROUP_PASSWORD;
	
	public GroupConfig() {
	}
	
	public GroupConfig(final String name) {
		setName(name);
	}
	
	public GroupConfig(final String name, final String password) {
		setName(name);
		setPassword(password);
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
	public GroupConfig setName(final String name) {
		if(name == null) {
			throw new NullPointerException("group name cannot be null");
		}

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
	public GroupConfig setPassword(final String password) {
		if(password == null) {
			throw new NullPointerException("group password cannot be null");
		}
		
		this.password = password;
		return this;
	}
	
}
