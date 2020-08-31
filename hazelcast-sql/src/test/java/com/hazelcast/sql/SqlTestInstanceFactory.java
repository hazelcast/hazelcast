/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;

import javax.annotation.Nonnull;

/**
 * An abstract factory that creates either JetInstances or
 * HazelcastInstances, based on the {@link #isJet} flag. Used to run SQL
 * tests on a jet cluster from Jet.
 * <p>
 * The JetInstances are created using reflection. {@code
 * JetInstance.getHazelcastInstance()} is returned.
 */
public abstract class SqlTestInstanceFactory {

    public static boolean isJet;

    private SqlTestInstanceFactory() { }

    public static SqlTestInstanceFactory create() {
        return isJet ? new JetInstanceFactory() : new ImdgInstanceFactory();
    }

    public abstract HazelcastInstance newHazelcastInstance();
    public abstract HazelcastInstance newHazelcastInstance(Config config);
    public abstract HazelcastInstance newHazelcastClient(ClientConfig clientConfig);
    public abstract void shutdownAll();

    /**
     * A factory for non-Jet clusters.
     */
    private static class ImdgInstanceFactory extends SqlTestInstanceFactory {

        private final TestHazelcastFactory factory = new TestHazelcastFactory();

        @Override
        public HazelcastInstance newHazelcastInstance() {
            return factory.newHazelcastInstance();
        }

        @Override
        public HazelcastInstance newHazelcastInstance(Config config) {
            return factory.newHazelcastInstance(config);
        }

        @Override
        public HazelcastInstance newHazelcastClient(ClientConfig clientConfig) {
            return factory.newHazelcastClient(clientConfig);
        }

        @Override
        public void shutdownAll() {
            factory.shutdownAll();
        }
    }

    /**
     * A factory for Jet clusters.
     */
    private static class JetInstanceFactory extends SqlTestInstanceFactory {

        private final Object jetFactory;

        JetInstanceFactory() {
            try {
                Class<?> jetFactoryClass = Class.forName("com.hazelcast.jet.JetTestInstanceFactory");
                jetFactory = jetFactoryClass.getConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public HazelcastInstance newHazelcastInstance() {
            try {
                Object jetInstance = jetFactory.getClass().getMethod("newMember")
                                             .invoke(jetFactory);
                return (HazelcastInstance) jetInstance.getClass().getMethod("getHazelcastInstance").invoke(jetInstance);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public HazelcastInstance newHazelcastInstance(@Nonnull Config config) {
            try {
                Object jetInstance = jetFactory.getClass().getMethod("newMember", config.getClass())
                                               .invoke(jetFactory, config);
                return (HazelcastInstance) jetInstance.getClass().getMethod("getHazelcastInstance").invoke(jetInstance);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public HazelcastInstance newHazelcastClient(ClientConfig clientConfig) {
            try {
                if (clientConfig != null) {
                    clientConfig.setClusterName("jet");
                }
                Object jetInstance = jetFactory.getClass().getMethod("newClient", clientConfig.getClass())
                                               .invoke(jetFactory, clientConfig);
                return (HazelcastInstance) jetInstance.getClass().getMethod("getHazelcastInstance").invoke(jetInstance);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void shutdownAll() {
            try {
                jetFactory.getClass().getMethod("shutdownAll")
                          .invoke(jetFactory);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
