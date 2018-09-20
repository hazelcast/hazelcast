/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.diagnostics;

import org.junit.Before;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.ProbeRegistry;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeRenderContext;

public abstract class DefaultMetricsTest extends AbstractMetricsTest {

    protected HazelcastInstance hz;
    private ProbeRegistry registry;
    private ProbeRenderContext renderContext;

    protected Config configure() {
        return new Config().setProperty(Diagnostics.METRICS_LEVEL.getName(),
                ProbeLevel.INFO.name());
    }

    @Before
    public void setup() {
        hz = createHazelcastInstance(configure());
        registry = getNode(hz).nodeEngine.getProbeRegistry();
        renderContext = registry.newRenderContext();
        warmUpPartitions(hz);
    }

    @Override
    protected final ProbeRenderContext getRenderContext() {
        return renderContext;
    }

}
