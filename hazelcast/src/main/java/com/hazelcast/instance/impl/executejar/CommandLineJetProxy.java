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

package com.hazelcast.instance.impl.executejar;

import com.hazelcast.instance.impl.BootstrappedJetProxy;
import com.hazelcast.jet.JetService;

import javax.annotation.Nonnull;

/**
 * This class' state holds {@link ExecuteJobParameters parameters} used by Jet jobs invoked via command line.
 */
public class CommandLineJetProxy<M> extends BootstrappedJetProxy<M> {

    private ExecuteJobParameters executeJobParameters;

    public CommandLineJetProxy(@Nonnull JetService jetService) {
        super(jetService);
    }

    public boolean hasExecuteJobParameters() {
        return getExecuteJobParameters() != null;
    }

    @Override
    public ExecuteJobParameters getExecuteJobParameters() {
        return executeJobParameters;
    }

    @Override
    public void setExecuteJobParameters(ExecuteJobParameters executeJobParameters) {
        this.executeJobParameters = executeJobParameters;
    }
}
