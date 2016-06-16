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
import com.hazelcast.jet.impl.application.Initable;
import com.hazelcast.jet.impl.hazelcast.JetService;
import com.hazelcast.jet.impl.statemachine.application.ApplicationState;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.config.JetApplicationConfig;

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
     * @param hazelcastInstance Hazelcast instance to use
     * @param applicationName name of the application
     * @return a new Jet Application
     */
    public static Application getJetApplication(HazelcastInstance hazelcastInstance,
                                                String applicationName) {
        return getJetApplication(hazelcastInstance, applicationName, null);
    }

    /**
     * Create a new application given a Hazelcast instance, name and application configuration
     * @param hazelcastInstance Hazelcast instance to use
     * @param applicationName name of the application
     * @param jetApplicationConfig configuration for the application
     * @return a new Jet Application
     */
    public static Application getJetApplication(HazelcastInstance hazelcastInstance,
                                                String applicationName,
                                                JetApplicationConfig jetApplicationConfig) {
        checkApplicationName(applicationName);

        Application application = hazelcastInstance.getDistributedObject(
                JetService.SERVICE_NAME,
                applicationName
        );

        if (application.getApplicationState() == ApplicationState.NEW) {
            ((Initable) application).init(jetApplicationConfig);
        }

        return application;
    }
}
