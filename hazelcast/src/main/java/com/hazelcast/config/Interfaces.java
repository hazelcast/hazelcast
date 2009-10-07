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
     * @return the lsInterfaces
     */
    public List<String> getLsInterfaces() {
        return lsInterfaces;
    }

    /**
     * @param lsInterfaces the lsInterfaces to set
     */
    public void setLsInterfaces(List<String> lsInterfaces) {
        this.lsInterfaces = lsInterfaces;
    }
}