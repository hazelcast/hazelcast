/**
 * 
 */
package com.hazelcast.cluster;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ConnectionManager;

public class Bind extends Master {

    public Bind() {
    }

    public Bind(Address localAddress) {
        super(localAddress);
    }

    @Override
    public String toString() {
        return "Bind " + address;
    }

    public void process() {
        ConnectionManager.get().bind(address, getConnection(), true);
    }
    
}