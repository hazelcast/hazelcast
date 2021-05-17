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

package com.hazelcast.jet.impl;

import com.hazelcast.instance.impl.HazelcastBootstrap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;

import javax.annotation.Nonnull;

/**
 * This class shouldn't be directly used, instead see {@link Jet#bootstrappedInstance()}
 * for the replacement and docs.
 * <p>
 * A helper class that allows one to create a standalone runnable JAR which
 * contains all the code needed to submit a job to a running Jet cluster.
 * The main issue with achieving this is that the JAR must be attached as a
 * resource to the job being submitted, so the Jet cluster will be able to
 * load and use its classes. However, from within a running {@code main()}
 * method it is not trivial to find out the filename of the JAR containing
 * it.
 **/
public final class JetBootstrap {

    private JetBootstrap() {
    }

    /**
     * Returns the bootstrapped {@code JetInstance}. The instance will be
     * automatically shut down once the {@code main()} method of the JAR returns.
     */
    @Nonnull
    public static JetInstance getInstance() {
        return HazelcastBootstrap.getInstance().getJetInstance();
    }
}
