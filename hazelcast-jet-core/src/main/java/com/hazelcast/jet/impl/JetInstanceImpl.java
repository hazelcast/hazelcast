/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.operation.ExecuteJobOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.util.concurrent.Future;

public class JetInstanceImpl extends AbstractJetInstance {
    private final NodeEngine nodeEngine;
    private final JetConfig config;
    private final ILogger logger;

    public JetInstanceImpl(HazelcastInstanceImpl hazelcastInstance, JetConfig config) {
        super(hazelcastInstance);
        this.nodeEngine = hazelcastInstance.node.getNodeEngine();
        this.logger = nodeEngine.getLogger(JetInstance.class);
        this.config = config;
    }

    @Override
    public JetConfig getConfig() {
        return config;
    }

    @Override
    public Job newJob(DAG dag) {
        return newJob(dag, new JobConfig());
    }

    @Override
    public Job newJob(DAG dag, JobConfig config) {
        return new JobImpl(dag, config);
    }

    private class JobImpl implements Job {

        private final DAG dag;
        private final JobConfig config;

        JobImpl(DAG dag, JobConfig config) {
            this.dag = dag;
            this.config = config;
        }

        @Override
        public Future<Void> execute() {
            long executionId = getIdGenerator().newId();
            new ResourceUploader(JetInstanceImpl.this.getMap(JetService.METADATA_MAP_PREFIX + executionId))
                    .uploadMetadata(config);

            Operation op = new ExecuteJobOperation(executionId, dag);
            return nodeEngine.getOperationService()
                             .createInvocationBuilder(JetService.SERVICE_NAME, op, nodeEngine.getThisAddress())
                             .invoke();
        }
    }
}
