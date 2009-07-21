/**
 * 
 */
package com.hazelcast.config;

public class TopicConfig {

	public final static boolean DEFAULT_GLOBAL_ORDERING_ENABLED = false;
	
    private String name;

    private boolean globalOrderingEnabled = DEFAULT_GLOBAL_ORDERING_ENABLED;

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the globalOrderingEnabled
	 */
	public boolean isGlobalOrderingEnabled() {
		return globalOrderingEnabled;
	}

	/**
	 * @param globalOrderingEnabled the globalOrderingEnabled to set
	 */
	public void setGlobalOrderingEnabled(boolean globalOrderingEnabled) {
		this.globalOrderingEnabled = globalOrderingEnabled;
	}

}