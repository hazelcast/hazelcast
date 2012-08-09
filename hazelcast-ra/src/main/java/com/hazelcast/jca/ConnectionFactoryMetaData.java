/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jca;

import javax.resource.cci.ResourceAdapterMetaData;

final class ConnectionFactoryMetaData implements ResourceAdapterMetaData {
	private static final Package HZ_PACKAGE = ConnectionFactoryImpl.class.getPackage();

	public String getAdapterName() {
		return HZ_PACKAGE.getImplementationTitle();
	}

	public String getAdapterShortDescription() {
		return HZ_PACKAGE.getSpecificationTitle();
	}

	public String getAdapterVendorName() {
		return HZ_PACKAGE.getImplementationVendor();
	}

	public String getAdapterVersion() {
		return HZ_PACKAGE.getImplementationVersion();
	}

	public String[] getInteractionSpecsSupported() {
		return new String[] {};
	}

	public String getSpecVersion() {
		return HZ_PACKAGE.getSpecificationVersion();
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