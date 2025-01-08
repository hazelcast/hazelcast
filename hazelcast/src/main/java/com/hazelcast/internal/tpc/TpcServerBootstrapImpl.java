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

package com.hazelcast.internal.tpc;

import com.hazelcast.config.tpc.TpcSocketConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.tpcengine.TpcEngine;

import java.util.Collections;
import java.util.List;

/**
 * The Open Source version of the {@link TpcServerBootstrap}. Since TPC is not enabled
 * in Open Source, this implementation doesn't do much.
 */
public class TpcServerBootstrapImpl implements TpcServerBootstrap {

    private final Node node;

    public TpcServerBootstrapImpl(Node node) {
        this.node = node;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void start() {
        // deal with the nonsense of mocking
        if (node == null || node.getConfig() == null) {
            return;
        }

        if (TpcServerBootstrap.loadTpcEnabled(node.getConfig())) {
            throw new IllegalStateException("Hazelcast Enterprise is required for TPC.");
        }
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public int eventloopCount() {
        return 0;
    }

    @Override
    public List<Integer> getClientPorts() {
        return Collections.emptyList();
    }

    @Override
    public TpcEngine getTpcEngine() {
        return null;
    }

    @Override
    public TpcSocketConfig getClientSocketConfig() {
        return node.getConfig().getNetworkConfig().getTpcSocketConfig();
    }
}
