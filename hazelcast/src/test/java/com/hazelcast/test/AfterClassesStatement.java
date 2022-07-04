/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.runners.model.Statement;

import java.util.Collection;
import java.util.Set;

import static com.hazelcast.cache.jsr.JsrTestUtil.clearCachingProviderRegistry;
import static com.hazelcast.cache.jsr.JsrTestUtil.getCachingProviderRegistrySize;

class AfterClassesStatement extends Statement {
    private final Statement originalStatement;

    AfterClassesStatement(Statement originalStatement) {
        this.originalStatement = originalStatement;
    }

    @Override
    public void evaluate() throws Throwable {
        originalStatement.evaluate();

        // check for running Hazelcast instances
        Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
        if (!instances.isEmpty()) {
            String message = "Instances haven't been shut down: " + instances;
            Hazelcast.shutdownAll();
            throw new IllegalStateException(message);
        }
        Collection<HazelcastInstance> clientInstances = HazelcastClient.getAllHazelcastClients();
        if (!clientInstances.isEmpty()) {
            String message = "Client instances haven't been shut down: " + clientInstances;
            HazelcastClient.shutdownAll();
            throw new IllegalStateException(message);
        }

        // check for leftover JMX beans
        JmxLeakHelper.checkJmxBeans();

        // check for leftover CachingProvider instances
        int registrySize = getCachingProviderRegistrySize();
        if (registrySize > 0) {
            clearCachingProviderRegistry();
            throw new IllegalStateException(registrySize + " CachingProviders are not cleaned up."
                                            + " Please use JsrTestUtil.cleanup() in your test!");
        }
    }
}
