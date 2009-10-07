/**
 *
 */
package com.hazelcast.config;

public class Join {

    private MulticastConfig multicastConfig = new MulticastConfig();

    private JoinMembers joinMembers = new JoinMembers();

    /**
     * @return the multicastConfig
     */
    public MulticastConfig getMulticastConfig() {
        return multicastConfig;
    }

    /**
     * @param multicastConfig the multicastConfig to set
     */
    public void setMulticastConfig(MulticastConfig multicastConfig) {
        this.multicastConfig = multicastConfig;
    }

    /**
     * @return the joinMembers
     */
    public JoinMembers getJoinMembers() {
        return joinMembers;
    }

    /**
     * @param joinMembers the joinMembers to set
     */
    public void setJoinMembers(JoinMembers joinMembers) {
        this.joinMembers = joinMembers;
    }
}