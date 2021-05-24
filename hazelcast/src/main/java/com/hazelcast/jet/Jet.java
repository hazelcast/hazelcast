/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JetBootstrap;

import javax.annotation.Nonnull;

/**
 * Entry point to the Jet product.
 *
 * @since Jet 3.0
 * @deprecated After 5.0 merge of Hazelcast products (IMDG and Jet into single
 * Hazelcast product), use the {@link Hazelcast} class as the entry point.
 */
@Deprecated
public final class Jet {

    private Jet() {
    }

    /**
     * @since Jet 4.0
     * @deprecated since 5.0
     * Please use {@link Hazelcast#bootstrappedInstance()} and then get
     * {@link JetInstance} from the created {@link HazelcastInstance}
     * by using {@link HazelcastInstance#getJetInstance()}.
     */
    @Nonnull
    @Deprecated
    public static JetInstance bootstrappedInstance() {
        return Hazelcast.bootstrappedInstance().getJetInstance();
    }

    /**
     * Creates a member of the Jet cluster with the configuration loaded from
     * default location.
     * @deprecated Use {@link Hazelcast#newHazelcastInstance().getJet()} instead.
     */
    @Deprecated
    @Nonnull
    public static JetInstance newJetInstance() {
        return (JetInstance) Hazelcast.newHazelcastInstance().getJet();
    }

    /**
     * Creates a member of the Jet cluster with the given configuration.
     * @deprecated use {@link Hazelcast#newHazelcastInstance(Config)} ()}
     */
    @Deprecated
    @Nonnull
    public static JetInstance newJetInstance(@Nonnull JetConfig config) {
        Preconditions.checkNotNull(config, "config");
        Config hzConfig = new Config();
        return (JetInstance) Hazelcast.newHazelcastInstance(hzConfig).getJet();
    }

    /**
     * Creates a Jet client with the default configuration.
     */
    @Deprecated
    @Nonnull
    public static JetInstance newJetClient() {
        return (JetInstance) HazelcastClient.newHazelcastClient().getJet();
    }

    /**
     * Creates a Jet client with the given Hazelcast client configuration.
     * <p>
     */
    @Deprecated
    @Nonnull
    public static JetInstance newJetClient(@Nonnull ClientConfig config) {
        Preconditions.checkNotNull(config, "config");
        return (JetInstance) HazelcastClient.newHazelcastClient(config).getJet();
    }

    /**
     * Creates a Jet client with cluster failover capability. Client will try to connect
     * to alternative clusters according to the supplied {@link ClientFailoverConfig}
     * when it disconnects from a cluster.
     */
    @Deprecated
    @Nonnull
    public static JetInstance newJetFailoverClient(@Nonnull ClientFailoverConfig config) {
        Preconditions.checkNotNull(config, "config");
        return (JetInstance) HazelcastClient.newHazelcastFailoverClient(config).getJet();
    }

    /**
     * Creates a Jet client with cluster failover capability. The client will
     * try to connect to alternative clusters as specified in the resolved {@link
     * ClientFailoverConfig} when it disconnects from a cluster.
     * <p>
     * The failover configuration is loaded using the following resolution mechanism:
     * <ol>
     * <li>System property {@code hazelcast.client.failover.config} is checked. If found,
     * and begins with {@code classpath:}, then a classpath resource is loaded, otherwise
     * it will be loaded from the file system. The configuration can be either an XML or a YAML
     * file, distinguished by the suffix of the provided file</li>
     * <li>{@code hazelcast-client-failover.xml} is checked on in the working dir</li>
     * <li>{@code hazelcast-client-failover.xml} is checked on the classpath</li>
     * <li>{@code hazelcast-client-failover.yaml} is checked on the working dir</li>
     * <li>{@code hazelcast-client-failover.yaml} is checked on the classpath</li>
     * <li>If none are available, then a {@link HazelcastException} is thrown</li>
     * </ol>
     */
    @Deprecated
    @Nonnull
    public static JetInstance newJetFailoverClient() {
        return (JetInstance) HazelcastClient.newHazelcastFailoverClient().getJet();
    }
}
