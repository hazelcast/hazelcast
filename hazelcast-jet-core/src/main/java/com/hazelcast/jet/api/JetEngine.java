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

package com.hazelcast.jet.api;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.internal.api.application.Initable;
import com.hazelcast.jet.internal.api.hazelcast.JetService;
import com.hazelcast.jet.internal.api.statemachine.application.ApplicationState;
import com.hazelcast.jet.internal.impl.util.JetUtil;
import com.hazelcast.jet.api.config.JetApplicationConfig;

public final class JetEngine {
    private JetEngine() {
    }

    private static void checkApplicationName(String applicationName) {
        JetUtil.checkApplicationName(applicationName);
    }

    public static Application getJetApplication(HazelcastInstance hazelcastInstance,
                                                String applicationName) {
        return getJetApplication(hazelcastInstance, applicationName, null);
    }

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
