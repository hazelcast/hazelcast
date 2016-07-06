/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.application.Application;
import com.hazelcast.jet.config.ApplicationConfig;
import com.hazelcast.jet.impl.application.ApplicationProxy;
import com.hazelcast.jet.impl.application.ApplicationService;
import com.hazelcast.jet.impl.application.client.ClientApplicationProxy;
import com.hazelcast.jet.impl.statemachine.application.ApplicationState;
import com.hazelcast.jet.impl.util.JetUtil;

/**
 * Utility class for creating new Jet Applications
 */
public final class JetEngine {
    private JetEngine() {
    }

    private static void checkApplicationName(String applicationName) {
        JetUtil.checkApplicationName(applicationName);
    }

    /**
     * Create a new application given a Hazelcast instance and name
     *
     * @param hazelcastInstance Hazelcast instance to use
     * @param name   name of the application
     * @return a new Jet Application
     */
    public static Application getApplication(HazelcastInstance hazelcastInstance,
                                             String name) {
        return getApplication(hazelcastInstance, name, null);
    }

    /**
     * Create a new application given a Hazelcast instance, name and application configuration
     *
     * @param hazelcastInstance Hazelcast instance to use
     * @param name   name of the application
     * @param applicationConfig configuration for the application
     * @return a new Jet Application
     */
    public static Application getApplication(HazelcastInstance hazelcastInstance,
                                             String name,
                                             ApplicationConfig applicationConfig) {
        checkApplicationName(name);

        Application application = hazelcastInstance.getDistributedObject(
                ApplicationService.SERVICE_NAME,
                name
        );

        // TODO: application init should be done in createDistributedObject
        if (application.getApplicationState() == ApplicationState.NEW) {
            if (application instanceof ApplicationProxy) {
                ((ApplicationProxy) application).init(applicationConfig);
            } else {
                ((ClientApplicationProxy) application).init(applicationConfig);
            }
        }

        return application;
    }
}
