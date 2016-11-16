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

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.jet2.impl.JetEngineProxyImpl;
import com.hazelcast.jet2.impl.client.ClientJetEngineProxy;

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
    static JetEngine get(HazelcastInstance instance, String name) {
        return get(instance, name, new JetEngineConfig());
    }

    /**
     * @return the {@code JetEngine} with the given name
     */
    static JetEngine get(HazelcastInstance instance, String name, JetEngineConfig config) {
        if (instance instanceof HazelcastInstanceImpl) {
            return JetEngineProxyImpl.createEngine(name, config, (HazelcastInstanceImpl) instance);
        }
        if (instance instanceof HazelcastInstanceProxy) {
            return JetEngineProxyImpl.createEngine(name, config, ((HazelcastInstanceProxy) instance).getOriginal());
        }
        if (instance instanceof HazelcastClientInstanceImpl) {
            return ClientJetEngineProxy.createEngine(name, config, (HazelcastClientInstanceImpl) instance);
        }
        return ClientJetEngineProxy.createEngine(name, config, ((HazelcastClientProxy) instance).client);
    }
}
