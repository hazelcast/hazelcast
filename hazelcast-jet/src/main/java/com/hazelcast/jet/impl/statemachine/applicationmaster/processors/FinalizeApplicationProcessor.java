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

package com.hazelcast.jet.impl.statemachine.applicationmaster.processors;

import com.hazelcast.jet.api.Dummy;
import com.hazelcast.jet.api.container.ProcessingContainer;
import com.hazelcast.jet.api.executor.ApplicationTaskContext;
import com.hazelcast.jet.api.container.ContainerPayLoadProcessor;
import com.hazelcast.jet.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.impl.util.JetUtil;


public class FinalizeApplicationProcessor implements ContainerPayLoadProcessor<Dummy> {
    private final ApplicationMaster applicationMaster;
    private final ApplicationTaskContext applicationTaskContext;

    public FinalizeApplicationProcessor(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
        this.applicationTaskContext = applicationMaster.getApplicationContext().getExecutorContext().getNetworkTaskContext();
    }

    @Override
    public void process(Dummy payload) throws Exception {
        Throwable error = null;

        try {
            for (ProcessingContainer container : this.applicationMaster.containers()) {
                try {
                    container.destroy();
                } catch (Throwable e) {
                    error = e;
                }
            }

            if (error != null) {
                throw JetUtil.reThrow(error);
            }
        } finally {
            this.applicationTaskContext.destroy();
        }
    }
}
