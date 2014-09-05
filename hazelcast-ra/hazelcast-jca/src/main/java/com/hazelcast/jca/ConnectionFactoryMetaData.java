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

import javax.resource.cci.ResourceAdapterMetaData;

/**
 * Simple implementation to return Hazelcast's implementation-
 * specific information in a JCA-compatible format
 */
final class ConnectionFactoryMetaData implements ResourceAdapterMetaData {
    /**
     * JCA-Connector specific java packge to be used for all information retrieval
     */
    private static final Package HZ_PACKAGE = ConnectionFactoryImpl.class.getPackage();

    /**
     * @return the implementation title from Hazelast
     */
    public String getAdapterName() {
        return HZ_PACKAGE.getImplementationTitle();
    }

    /**
     * @return the specification title from Hazelast
     */
    public String getAdapterShortDescription() {
        return HZ_PACKAGE.getSpecificationTitle();
    }

    /**
     * @return Hazelcast's implementation vendor
     */
    public String getAdapterVendorName() {
        return HZ_PACKAGE.getImplementationVendor();
    }

    /**
     * @return Hazelcast's implementation version
     */
    public String getAdapterVersion() {
        return HZ_PACKAGE.getImplementationVersion();
    }

    /**
     * There is no real specification thus always an empty String array...
     */
    public String[] getInteractionSpecsSupported() {
        return new String[]{};
    }

    /**
     * @return Hazelcast's specification version
     */
    public String getSpecVersion() {
        return HZ_PACKAGE.getSpecificationVersion();
    }

    /* (non-Javadoc)
     * @see javax.resource.cci.ResourceAdapterMetaData#supportsExecuteWithInputAndOutputRecord()
     */
    public boolean supportsExecuteWithInputAndOutputRecord() {
        return false;
    }

    /* (non-Javadoc)
     * @see javax.resource.cci.ResourceAdapterMetaData#supportsExecuteWithInputRecordOnly()
     */
    public boolean supportsExecuteWithInputRecordOnly() {
        return false;
    }

    /* (non-Javadoc)
     * @see javax.resource.cci.ResourceAdapterMetaData#supportsLocalTransactionDemarcation()
     */
    public boolean supportsLocalTransactionDemarcation() {
        return false;
    }
}
