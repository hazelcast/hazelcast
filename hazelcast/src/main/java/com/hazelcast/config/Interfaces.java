/**
 *
 */
package com.hazelcast.config;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class Interfaces {

    private boolean enabled = false;

    private final Set<String> interfaceSet = new HashSet<String>();

    /**
     * @return the enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @param enabled the enabled to set
     */
    public void setEnabled(final boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Adds a new interface
     * @param ip
     */
    public void addInterface(final String ip) {
       	interfaceSet.add(ip);
    }

    /**
     * clears all interfaces.
     */
    public void clear() {
    	interfaceSet.clear();
    }

    /**
     * @return a read-only collection of interfaces
     */
    public Collection<String> getInterfaces() {
        return Collections.unmodifiableCollection(interfaceSet);
    }

    /**
     * Adds a collection of interfaces.
     * Clears the current collection and then adds all entries of new collection.
     * 
     * @param interfaces the interfaces to set
     */
    public void setInterfaces(final Collection<String> interfaces) {
    	clear();
    	this.interfaceSet.addAll(interfaces);
    }
}