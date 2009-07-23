/**
 * 
 */
package com.hazelcast.config;

public class MulticastConfig {

	public final static boolean DEFAULT_ENABLED = true;
	public final static String DEFAULT_MULTICAST_GROUP = "224.2.2.3";
	public final static int DEFAULT_MULTICAST_PORT = 54327;
	
    private boolean enabled = DEFAULT_ENABLED;

    private String multicastGroup = DEFAULT_MULTICAST_GROUP;

    private int multicastPort = DEFAULT_MULTICAST_PORT;

	/**
	 * @return the enabled
	 */
	public boolean isEnabled() {
		return enabled;
	}

	/**
	 * @param enabled the enabled to set
	 */
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	/**
	 * @return the multicastGroup
	 */
	public String getMulticastGroup() {
		return multicastGroup;
	}

	/**
	 * @param multicastGroup the multicastGroup to set
	 */
	public void setMulticastGroup(String multicastGroup) {
		this.multicastGroup = multicastGroup;
	}

	/**
	 * @return the multicastPort
	 */
	public int getMulticastPort() {
		return multicastPort;
	}

	/**
	 * @param multicastPort the multicastPort to set
	 */
	public void setMulticastPort(int multicastPort) {
		this.multicastPort = multicastPort;
	}
}