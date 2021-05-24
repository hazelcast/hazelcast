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
import com.hazelcast.core.HazelcastInstance;

import javax.annotation.Nonnull;

/**
 * Entry point to the Jet product.
 *
 * @since Jet 3.0
 * @deprecated After 5.0 merge of Hazelcast products (IMDG and Jet into single
 * Hazelcast product), use the {@link Hazelcast} class as the entry point.
 */
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
}
