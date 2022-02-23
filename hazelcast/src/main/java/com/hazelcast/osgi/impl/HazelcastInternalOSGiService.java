/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.osgi.impl;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.osgi.HazelcastOSGiService;

/**
 * Contract point for internal Hazelcast services on top of OSGI.
 *
 * @see com.hazelcast.osgi.HazelcastOSGiService
 */
public interface HazelcastInternalOSGiService
        extends HazelcastOSGiService {

    /**
     * Default ID for {@link HazelcastInternalOSGiService} instance
     */
    String DEFAULT_ID =
            BuildInfoProvider.getBuildInfo().getVersion()
            + "#"
            + (BuildInfoProvider.getBuildInfo().isEnterprise() ? "EE" : "OSS");

    /**
     * Default cluster name to be used when grouping is not disabled with
     * {@link HazelcastOSGiService#HAZELCAST_OSGI_GROUPING_DISABLED}.
     */
    String DEFAULT_CLUSTER_NAME = DEFAULT_ID;

    /**
     * Returns the state of the service about if it is active or not.
     *
     * @return <code>true</code> if the service is active, otherwise <code>false</code>
     */
    boolean isActive();

    /**
     * Activates the service.
     */
    void activate();

    /**
     * Deactivates the service.
     */
    void deactivate();

}
