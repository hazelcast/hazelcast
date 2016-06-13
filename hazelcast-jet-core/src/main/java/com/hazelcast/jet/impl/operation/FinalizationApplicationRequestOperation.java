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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.jet.impl.JetApplicationManager;
import com.hazelcast.jet.impl.application.ApplicationContext;
import com.hazelcast.jet.impl.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.impl.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.jet.impl.hazelcast.JetService;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.FinalizeApplicationRequest;
import com.hazelcast.jet.impl.util.JetUtil;

public class FinalizationApplicationRequestOperation extends AsyncJetOperation {

    @SuppressWarnings("unused")
    public FinalizationApplicationRequestOperation() {

    }

    public FinalizationApplicationRequestOperation(String name) {
        super(name);
    }

    private void destroyApplication(ApplicationContext applicationContext) {
        try {
            shutDownExecutors(applicationContext);
        } catch (Exception e) {
            throw JetUtil.reThrow(e);
        } finally {
            try {
                applicationContext.getLocalizationStorage().cleanUp();
            } finally {
                JetService jetService = getService();
                JetApplicationManager jetApplicationManager = jetService.getApplicationManager();
                jetApplicationManager.destroyApplication(getApplicationName());
            }
        }
    }

    @Override
    public void run() throws Exception {
        ApplicationContext applicationContext = getApplicationContext();
        ApplicationMaster applicationMaster = applicationContext.getApplicationMaster();
        ICompletableFuture<ApplicationMasterResponse> future =
                applicationMaster.handleContainerRequest(new FinalizeApplicationRequest());

        future.andThen(new ExecutionCallback<ApplicationMasterResponse>() {
            @Override
            public void onResponse(ApplicationMasterResponse response) {
                if (!response.isSuccess()) {
                    sendResponse(new IllegalStateException("Unable to finalize application."));
                    return;
                }
                try {
                    destroyApplication(applicationContext);
                } catch (Exception e) {
                    sendResponse(e);
                    return;
                }
                sendResponse(true);
            }

            @Override
            public void onFailure(Throwable t) {
                sendResponse(t);
            }
        });
    }

    private void shutDownExecutors(ApplicationContext applicationContext) throws Exception {
        applicationContext.getExecutorContext().getApplicationStateMachineExecutor().shutdown();
        applicationContext.getExecutorContext().getDataContainerStateMachineExecutor().shutdown();
        applicationContext.getExecutorContext().getApplicationMasterStateMachineExecutor().shutdown();
    }
}
