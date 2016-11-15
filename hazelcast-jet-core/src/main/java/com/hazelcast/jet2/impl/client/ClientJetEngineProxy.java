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

package com.hazelcast.jet2.impl.client;

import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.JetEngine;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.Job;
import com.hazelcast.jet2.impl.JetEngineProxy;
import com.hazelcast.jet2.impl.JobImpl;

public class ClientJetEngineProxy extends ClientProxy implements JetEngineProxy {
    public ClientJetEngineProxy(String serviceName, String name) {
        super(serviceName, name);
    }

    @Override
    public Job newJob(DAG dag) {
        return new JobImpl(this, dag);
    }

    @Override
    public void deployResources() {

    }

    @Override
    public void execute(JobImpl job) {

    }

    public static JetEngine createEngine(String name, JetEngineConfig config, HazelcastClientProxy instance) {
        throw new UnsupportedOperationException();
    }
}

