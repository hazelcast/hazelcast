/**
 * 
 */
package com.hazelcast.jca;

import javax.resource.cci.ResourceAdapterMetaData;

final class ConnectionFactoryMetaData implements
		ResourceAdapterMetaData {
	public String getAdapterName() {
		return ConnectionFactoryImpl.class.getPackage().getImplementationTitle();
	}

	public String getAdapterShortDescription() {
		return ConnectionFactoryImpl.class.getPackage().getSpecificationTitle();
	}

	public String getAdapterVendorName() {
		return ConnectionFactoryImpl.class.getPackage().getImplementationVendor();
	}

	public String getAdapterVersion() {
		return ConnectionFactoryImpl.class.getPackage().getImplementationVersion();
	}

	public String[] getInteractionSpecsSupported() {
		return new String[0];
	}

	public String getSpecVersion() {
		return ConnectionFactoryImpl.class.getPackage().getSpecificationVersion();
	}

	public boolean supportsExecuteWithInputAndOutputRecord() {
		return false;
	}

	public boolean supportsExecuteWithInputRecordOnly() {
		return false;
	}

	public boolean supportsLocalTransactionDemarcation() {
		return false;
	}
}