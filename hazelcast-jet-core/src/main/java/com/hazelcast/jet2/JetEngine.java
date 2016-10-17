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

package com.hazelcast.jet2;

import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.jet2.impl.JetEngineImpl;
import com.hazelcast.jet2.impl.JetService;
import com.hazelcast.jet2.impl.deployment.CreateExecutionContextOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import java.util.ArrayList;
import java.util.Set;

/**
 * Javadoc pending
 */
public interface JetEngine extends DistributedObject {

    /**
     * @return a new {@link Job} with the given DAG
     */
    Job newJob(DAG dag);

    /**
     * @return the {@code JetEngine} with the given name
     */
    static JetEngine get(HazelcastInstance instance, String name, JetEngineConfig config) {
        HazelcastInstanceImpl instanceImpl = null;
        if (instance instanceof HazelcastInstanceProxy) {
            instanceImpl = ((HazelcastInstanceProxy) instance).getOriginal();
        } else if (instance instanceof HazelcastInstanceImpl) {
            instanceImpl = ((HazelcastInstanceImpl) instance);
        } else if (instance instanceof HazelcastClientProxy) {
            throw new UnsupportedOperationException();
        }
        if (instanceImpl != null) {
            NodeEngineImpl nodeEngine = instanceImpl.node.getNodeEngine();
            InternalOperationService operationService = nodeEngine.getOperationService();
            Set<Member> members = nodeEngine.getClusterService().getMembers();
            ArrayList<InternalCompletableFuture> futures = new ArrayList<>();
            for (Member member : members) {
                CreateExecutionContextOperation createExecutionContextOperation =
                        new CreateExecutionContextOperation(name, config);
                futures.add(operationService.invokeOnTarget(JetService.SERVICE_NAME,
                        createExecutionContextOperation, member.getAddress()));
            }
            for (InternalCompletableFuture future : futures) {
                future.join();
            }
        }
        JetEngineImpl jetEngine = instance.getDistributedObject(JetService.SERVICE_NAME, name);
        jetEngine.initializeDeployment();
        return jetEngine;
    }

    /**
     * @return the {@code JetEngine} with the given name
     */
    static JetEngine get(HazelcastInstance instance, String name) {
        return get(instance, name, new JetEngineConfig());
    }
}
