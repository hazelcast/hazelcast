/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.config.JetConfig;

import javax.annotation.Nonnull;

/**
 * Entry point to the Jet product.
 *
 * @since Jet 3.0
 *
 * @deprecated After 5.0 Jet was merged into core Hazelcast product. Use
 * the {@link Hazelcast} class as the entry point.
 */
@Deprecated(since = "5.0")
public final class Jet {

    private Jet() {
    }

    /**
     * @since Jet 4.0
     * @deprecated
     * Please use {@link Hazelcast#bootstrappedInstance()} and then get
     * {@link JetService} from the created {@link HazelcastInstance}
     * by using {@link HazelcastInstance#getJet()}}.
     */
    @Nonnull
    @Deprecated(since = "5.0")
    public static JetInstance bootstrappedInstance() {
        return (JetInstance) Hazelcast.bootstrappedInstance().getJet();
    }

    /**
     * @deprecated
     * Please use {@link Hazelcast#newHazelcastInstance()} and then get
     * {@link JetService} from the created {@link HazelcastInstance}
     * by using {@link HazelcastInstance#getJet()}}.
     */
    @Deprecated(since = "5.0")
    @Nonnull
    public static JetInstance newJetInstance() {
        return (JetInstance) Hazelcast.newHazelcastInstance().getJet();
    }

    /**
     * @deprecated
     * Use {@link Hazelcast#newHazelcastInstance(Config)} and then get
     * {@link JetService} from the created {@link HazelcastInstance} by
     * using {@link HazelcastInstance#getJet()}}.
     */
    @Deprecated(since = "5.0")
    @Nonnull
    public static JetInstance newJetInstance(@Nonnull JetConfig config) {
        Preconditions.checkNotNull(config, "config");
        Config hzConfig = new Config();
        hzConfig.setJetConfig(config);
        return (JetInstance) Hazelcast.newHazelcastInstance(hzConfig).getJet();
    }

    /**
     * @deprecated
     * Use {@link HazelcastClient#newHazelcastClient()} and then get
     * {@link JetService} from the created {@link HazelcastInstance}
     * client by using {@link HazelcastInstance#getJet()}}.
     */
    @Deprecated(since = "5.0")
    @Nonnull
    public static JetInstance newJetClient() {
        return (JetInstance) HazelcastClient.newHazelcastClient().getJet();
    }

    /**
     * @deprecated
     * Use {@link HazelcastClient#newHazelcastClient(ClientConfig)} and
     * then get {@link JetService} from the created {@link
     * HazelcastInstance} client by using {@link HazelcastInstance#getJet()}}.
     */
    @Deprecated(since = "5.0")
    @Nonnull
    public static JetInstance newJetClient(@Nonnull ClientConfig config) {
        Preconditions.checkNotNull(config, "config");
        return (JetInstance) HazelcastClient.newHazelcastClient(config).getJet();
    }

    /**
     * @deprecated
     * Use {@link HazelcastClient#newHazelcastFailoverClient()} and
     * then get {@link JetService} from the created {@link HazelcastInstance}
     * client by using {@link HazelcastInstance#getJet()}}.
     */
    @Deprecated(since = "5.0")
    @Nonnull
    public static JetInstance newJetFailoverClient() {
        return (JetInstance) HazelcastClient.newHazelcastFailoverClient().getJet();
    }

    /**
     * @deprecated
     * Use {@link HazelcastClient#newHazelcastFailoverClient(ClientFailoverConfig)}
     * and then get {@link JetService} from the created {@link HazelcastInstance}
     * client by using {@link HazelcastInstance#getJet()}}.
     */
    @Deprecated(since = "5.0")
    @Nonnull
    public static JetInstance newJetFailoverClient(@Nonnull ClientFailoverConfig config) {
        Preconditions.checkNotNull(config, "config");
        return (JetInstance) HazelcastClient.newHazelcastFailoverClient(config).getJet();
    }
}
