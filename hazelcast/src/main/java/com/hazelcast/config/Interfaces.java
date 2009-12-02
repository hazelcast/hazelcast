/**
 *
 */
package com.hazelcast.config;

import java.util.ArrayList;
import java.util.List;

public class Interfaces {

    private boolean enabled = false;

    private List<String> lsInterfaces = new ArrayList<String>();

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
     * Adds a new interface
     * @param ip
     */
    public void addInterface(String ip) {
        lsInterfaces.add(ip);
    }

    /**
     * clears all interfaces.
     */
    public void clear() {
        lsInterfaces.clear();
    }

    /**
     * @return the lsInterfaces
     */
    public List<String> getInterfaceList() {
        return lsInterfaces;
    }

    /**
     * @param lsInterfaces the lsInterfaces to set
     */
    public void setInterfaceList(List<String> lsInterfaces) {
        this.lsInterfaces = lsInterfaces;
    }
}