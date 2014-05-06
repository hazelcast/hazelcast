/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Implements the meta data of this hazelcast connections based on the implementation details
 */
final class ManagedConnectionMetaData implements
        javax.resource.spi.ManagedConnectionMetaData,
        javax.resource.cci.ConnectionMetaData {
    public ManagedConnectionMetaData() {
    }

    /* (non-Javadoc)
     * @see javax.resource.spi.ManagedConnectionMetaData#getEISProductName()
     */
    public String getEISProductName() throws ResourceException {
        return ManagedConnectionMetaData.class.getPackage().getImplementationTitle();
    }

    /* (non-Javadoc)
     * @see javax.resource.spi.ManagedConnectionMetaData#getEISProductVersion()
     */
    public String getEISProductVersion() throws ResourceException {
        return ManagedConnectionMetaData.class.getPackage().getImplementationVersion();
    }

    /* (non-Javadoc)
     * @see javax.resource.spi.ManagedConnectionMetaData#getMaxConnections()
     */
    public int getMaxConnections() throws ResourceException {
        // The heap is the limit...
        return Integer.MAX_VALUE;
    }

    /* (non-Javadoc)
     * @see javax.resource.spi.ManagedConnectionMetaData#getUserName()
     */
    public String getUserName() throws ResourceException {
        // No security here
        return "";
    }
}
