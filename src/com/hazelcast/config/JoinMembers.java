/**
 * 
 */
package com.hazelcast.config;

import java.util.ArrayList;
import java.util.List;

public class JoinMembers {
    private int connectionTimeoutSeconds = 5;

    private boolean enabled = false;

    private List<String> lsMembers = new ArrayList<String>();

    private String requiredMember = null;

    public void add(final String member) {
        lsMembers.add(member);
    }

	/**
	 * @return the connectionTimeoutSeconds
	 */
	public int getConnectionTimeoutSeconds() {
		return connectionTimeoutSeconds;
	}

	/**
	 * @param connectionTimeoutSeconds the connectionTimeoutSeconds to set
	 */
	public void setConnectionTimeoutSeconds(int connectionTimeoutSeconds) {
		this.connectionTimeoutSeconds = connectionTimeoutSeconds;
	}

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
	 * @return the lsMembers
	 */
	public List<String> getLsMembers() {
		return lsMembers;
	}

	/**
	 * @param lsMembers the lsMembers to set
	 */
	public void setLsMembers(List<String> lsMembers) {
		this.lsMembers = lsMembers;
	}

	/**
	 * @return the requiredMember
	 */
	public String getRequiredMember() {
		return requiredMember;
	}

	/**
	 * @param requiredMember the requiredMember to set
	 */
	public void setRequiredMember(String requiredMember) {
		this.requiredMember = requiredMember;
	}
}