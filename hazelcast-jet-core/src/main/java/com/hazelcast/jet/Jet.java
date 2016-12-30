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

package com.hazelcast.jet;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JetInstanceImpl;
import com.hazelcast.jet.impl.JetService;

/**
 * Javadoc pending
 */
public final class Jet {

    private Jet() {
    }

    /**
     * Javadoc pending
     *
     * @param config
     * @return
     */
    public static JetInstance newJetInstance(JetConfig config) {
        config.getHazelcastConfig().getServicesConfig()
              .addServiceConfig(new ServiceConfig().setEnabled(true)
                                                   .setName(JetService.SERVICE_NAME)
                                                   .setClassName(JetService.class.getName())
                                                   .setConfigObject(config));
        HazelcastInstanceImpl hazelcastInstance = ((HazelcastInstanceProxy)
                Hazelcast.newHazelcastInstance(config.getHazelcastConfig())).getOriginal();
        return new JetInstanceImpl(hazelcastInstance, config);
    }

    /**
     * Javadoc pending
     *
     * @return
     */
    public static JetInstance newJetInstance() {
        return newJetInstance(new JetConfig());
    }

    /**
     * @return
     */
    public static JetInstance newJetClient() {
        return getJetClientInstance(HazelcastClient.newHazelcastClient());
    }

    /**
     * @param config
     * @return
     */
    public static JetInstance newJetClient(ClientConfig config) {
        return getJetClientInstance(HazelcastClient.newHazelcastClient(config));
    }

    /**
     * Javadoc pending
     */
    public static void shutdownAll() {
        Hazelcast.shutdownAll();
    }

    static JetClientInstanceImpl getJetClientInstance(HazelcastInstance client) {
        return new JetClientInstanceImpl(((HazelcastClientProxy) client).client);
    }
}
