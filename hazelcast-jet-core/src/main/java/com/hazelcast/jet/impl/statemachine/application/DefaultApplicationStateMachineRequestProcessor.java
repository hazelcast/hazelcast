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

package com.hazelcast.jet.impl.statemachine.application;

import com.hazelcast.jet.api.statemachine.application.ApplicationEvent;
import com.hazelcast.jet.api.statemachine.application.ApplicationStateMachineRequestProcessor;
import com.hazelcast.jet.impl.application.ApplicationContextImpl;

public class DefaultApplicationStateMachineRequestProcessor implements ApplicationStateMachineRequestProcessor {
    private final ApplicationContextImpl applicationContext;

    public DefaultApplicationStateMachineRequestProcessor(ApplicationContextImpl applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public void processRequest(ApplicationEvent event, Object payLoad) throws Exception {
        if (event == ApplicationEvent.EXECUTION_START) {
            this.applicationContext.getExecutorContext().getNetworkTaskContext().init();
            this.applicationContext.getExecutorContext().getApplicationTaskContext().init();
        }

        if ((event == ApplicationEvent.EXECUTION_FAILURE)
                || (event == ApplicationEvent.EXECUTION_SUCCESS)
                || (event == ApplicationEvent.INTERRUPTION_FAILURE)
                || (event == ApplicationEvent.INTERRUPTION_SUCCESS)
                ) {
            this.applicationContext.getExecutorContext().getNetworkTaskContext().destroy();
        }
    }
}
