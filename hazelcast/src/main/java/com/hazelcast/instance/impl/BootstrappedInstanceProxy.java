/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.executejar.ExecuteJobParameters;
import com.hazelcast.jet.Job;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;
import java.util.List;

// A special HazelcastInstance that has a BootstrappedJetProxy
@SuppressWarnings({"checkstyle:methodcount"})
public final class BootstrappedInstanceProxy
        extends AbstractHazelcastInstanceProxy<HazelcastInstance> {

    private static final ILogger LOGGER = Logger.getLogger(BootstrappedInstanceProxy.class);

    private final BootstrappedJetProxy jetProxy;

    private boolean shutDownAllowed = true;

    BootstrappedInstanceProxy(HazelcastInstance instance, BootstrappedJetProxy jetProxy) {
        super(instance);
        this.jetProxy = jetProxy;
    }

    public List<Job> getSubmittedJobs() {
        ExecuteJobParameters executeJobParameters = jetProxy.getExecuteJobParameters();
        return executeJobParameters.getSubmittedJobs();
    }

    public BootstrappedInstanceProxy setShutDownAllowed(boolean shutDownAllowed) {
        this.shutDownAllowed = shutDownAllowed;
        return this;
    }

    public void setExecuteJobParameters(ExecuteJobParameters executeJobParameters) {
        jetProxy.setExecuteJobParameters(executeJobParameters);
    }

    public void removeExecuteJobParameters() {
        jetProxy.removeExecuteJobParameters();
    }

    @Nonnull
    @Override
    public BootstrappedJetProxy getJet() {
        return jetProxy;
    }

    @Override
    public void shutdown() {
        if (shutDownAllowed) {
            getLifecycleService().shutdown();
        } else {
            LOGGER.severe("Shutdown of BootstrappedInstanceProxy is not allowed");
        }
    }
}
