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
		return ManagedConnectionMetaData.class.getPackage().getImplementationVersion();
	}

	public int getMaxConnections() throws ResourceException {
		return Integer.MAX_VALUE;
	}

	public String getUserName() throws ResourceException {
		return "";
	}
}