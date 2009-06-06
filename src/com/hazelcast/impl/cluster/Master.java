/**
 * 
 */
package com.hazelcast.impl.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.Address;

/**
 * The Class Master.
 */
public class Master extends AbstractRemotelyProcessable {
	
    /** The address. */
    protected Address address = null;

    /**
	 * Instantiates a new master.
	 */
    public Master() {

    }

    /**
	 * Instantiates a new master.
	 * 
	 * @param originAddress
	 *            the origin address
	 */
    public Master(final Address originAddress) {
        super();
        this.address = originAddress;
    }

    /* (non-Javadoc)
     * @see com.hazelcast.impl.cluster.AbstractRemotelyProcessable#readData(java.io.DataInput)
     */
    @Override
    public void readData(final DataInput in) throws IOException {
        address = new Address();
        address.readData(in);
    }

    /* (non-Javadoc)
     * @see com.hazelcast.impl.cluster.AbstractRemotelyProcessable#writeData(java.io.DataOutput)
     */
    @Override
    public void writeData(final DataOutput out) throws IOException {
        address.writeData(out);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Master " + address;
    }

    /* (non-Javadoc)
     * @see com.hazelcast.impl.BaseManager.Processable#process()
     */
    public void process() {
        ClusterManager.get().handleMaster(this);
    }

	/**
	 * Gets the address.
	 * 
	 * @return the address
	 */
	public Address getAddress() {
		return address;
	}

	/**
	 * Sets the address.
	 * 
	 * @param address
	 *            the address to set
	 */
	public void setAddress(final Address address) {
		this.address = address;
	}
    
}