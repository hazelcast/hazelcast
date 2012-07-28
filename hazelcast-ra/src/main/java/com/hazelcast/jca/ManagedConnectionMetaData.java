/**
 * 
 */
package com.hazelcast.jca;

import javax.resource.ResourceException;
import javax.resource.cci.ConnectionMetaData;

final class ManagedConnectionMetaData implements javax.resource.spi.ManagedConnectionMetaData,
	ConnectionMetaData {
	
	public ManagedConnectionMetaData() {
	}
	
	
	public String getEISProductName() throws ResourceException {
		return ManagedConnectionMetaData.class.getPackage().getImplementationTitle();
	}

	public String getEISProductVersion() throws ResourceException {
		return ManagedConnectionMetaData.class.getPackage().getImplementationTitle();
	}

	public int getMaxConnections() throws ResourceException {
		return Integer.MAX_VALUE;
	}

	public String getUserName() throws ResourceException {
		return "";
	}
}