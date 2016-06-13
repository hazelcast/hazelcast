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
import com.hazelcast.jet.impl.application.ApplicationContext;
import com.hazelcast.jet.impl.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.impl.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecuteApplicationRequest;


public class ExecutionApplicationRequestOperation extends AsyncJetOperation {

    @SuppressWarnings("unused")
    public ExecutionApplicationRequestOperation() {
    }

    public ExecutionApplicationRequestOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {

        ApplicationContext applicationContext = getApplicationContext();
        ApplicationMaster applicationMaster = applicationContext.getApplicationMaster();

        getLogger().fine("ExecutionApplicationRequestOperation.run " + applicationContext.getName());

        ICompletableFuture<ApplicationMasterResponse> future = applicationMaster
                .handleContainerRequest(new ExecuteApplicationRequest());

        //Waiting for until all containers started
        future.andThen(new ContainerRequestCallback(this, "Unable to start containers", () -> {

            //Waiting for execution completion
            final ICompletableFuture<Object> mailboxFuture = applicationMaster.getExecutionMailBox();
            mailboxFuture.andThen(new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object o) {
                    sendResponse(o);
                }

                @Override
                public void onFailure(Throwable throwable) {

                    sendResponse(throwable);
                }
            });
        }));
    }
}
