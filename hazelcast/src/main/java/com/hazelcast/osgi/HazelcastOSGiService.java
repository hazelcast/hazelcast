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

package com.hazelcast.osgi;

import com.hazelcast.config.Config;
import org.osgi.framework.Bundle;

import java.util.Set;

/**
 * Contract point for Hazelcast services on top of OSGI.
 */
public interface HazelcastOSGiService {

    /**
     * System property for starting a default Hazelcast instance per Hazelcast OSGI bundle.
     */
    String HAZELCAST_OSGI_START = "hazelcast.osgi.start";

    /**
     * System property for disabling the behaviour that registers created Hazelcast instances as OSGI service.
     */
    String HAZELCAST_OSGI_REGISTER_DISABLED = "hazelcast.osgi.register.disabled";

    /**
     * System property for disabling the behaviour that gives different cluster name
     * to each Hazelcast bundle.
     */
    String HAZELCAST_OSGI_GROUPING_DISABLED = "hazelcast.osgi.grouping.disabled";

    /**
     * System property for disabling the JSR-223 script engine scan by the Hazelcast OSGI service.
     */
    String HAZELCAST_OSGI_JSR223_DISABLED = "hazelcast.osgi.jsr223.disabled";

    /**
     * Gets the ID of service.
     *
     * @return the ID of service
     */
    String getId();

    /**
     * Gets the owner {@link Bundle} of this instance.
     *
     * @return the owner {@link Bundle} of this instance
     */
    Bundle getOwnerBundle();

    /**
     * Gets the default {@link HazelcastOSGiInstance}.
     *
     * @return the default {@link HazelcastOSGiInstance}
     */
    HazelcastOSGiInstance getDefaultHazelcastInstance();

    /**
     * Creates a new {@link HazelcastOSGiInstance}
     * on the owner bundle with specified configuration.
     *
     * @param config Configuration for the new {@link HazelcastOSGiInstance} (member)
     * @return the new {@link HazelcastOSGiInstance}
     */
    HazelcastOSGiInstance newHazelcastInstance(Config config);

    /**
     * Creates a new {@link HazelcastOSGiInstance}
     * on the owner bundle with default configuration.
     *
     * @return the new {@link HazelcastOSGiInstance}
     */
    HazelcastOSGiInstance newHazelcastInstance();

    /**
     * Gets an existing {@link HazelcastOSGiInstance} with its <code>instanceName</code>.
     *
     * @param instanceName Name of the {@link HazelcastOSGiInstance} (member)
     * @return an existing {@link HazelcastOSGiInstance}
     */
    HazelcastOSGiInstance getHazelcastInstanceByName(String instanceName);

    /**
     * Gets all active/running {@link HazelcastOSGiInstance}s on the owner bundle.
     *
     * @return all active/running {@link HazelcastOSGiInstance}s on the owner bundle
     */
    Set<HazelcastOSGiInstance> getAllHazelcastInstances();

    /**
     * Shuts down the given {@link HazelcastOSGiInstance} on the owner bundle.
     *
     * @param instance the {@link HazelcastOSGiInstance} to shutdown
     */
    void shutdownHazelcastInstance(HazelcastOSGiInstance instance);

    /**
     * Shuts down all running {@link HazelcastOSGiInstance}s on the owner bundle.
     */
    void shutdownAll();

}
