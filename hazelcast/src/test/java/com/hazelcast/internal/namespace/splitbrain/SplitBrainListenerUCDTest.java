/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.namespace.splitbrain;

import com.hazelcast.config.Config;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.config.SplitBrainProtectionListenerConfig;
import com.hazelcast.internal.namespace.NamespaceService;
import com.hazelcast.internal.namespace.UCDTest;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionEvent;
import com.hazelcast.splitbrainprotection.impl.SplitBrainProtectionServiceImpl;
import org.junit.runners.Parameterized;

import java.util.Collections;
import java.util.stream.Collectors;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;

public class SplitBrainListenerUCDTest extends UCDTest {
    protected SplitBrainProtectionConfig splitBrainProtectionConfig;

    @Override
    protected void initialiseConfig() {
        splitBrainProtectionConfig = new SplitBrainProtectionConfig("my_sbp");
    }

    @Override
    protected void registerConfig(Config config) {
        config.addSplitBrainProtectionConfig(splitBrainProtectionConfig);
    }

    @Override
    public void test() throws Exception {
        // Fire a fake split brain protection event
        NodeEngineImpl nodeEngine = getNodeEngineImpl(member);
        SplitBrainProtectionEvent splitBrainProtectionEvent = new SplitBrainProtectionEvent(nodeEngine.getThisAddress(),
                2, Collections.emptyList(), false);
        nodeEngine.getEventService().publishEvent(SplitBrainProtectionServiceImpl.SERVICE_NAME, "my_sbp",
                splitBrainProtectionEvent, splitBrainProtectionEvent.hashCode());

        assertListenerFired("onChange");
    }

    @Override
    protected String getNamespaceName() {
        // Utilizing default Namespace
        return NamespaceService.DEFAULT_NAMESPACE_NAME;
    }

    @Override
    protected void initialiseDataStructure() throws Exception {
        // No data structure to initialise
    }

    @Override
    protected void addClassInstanceToConfig() throws ReflectiveOperationException, UnsupportedOperationException {
        SplitBrainProtectionListenerConfig config = new SplitBrainProtectionListenerConfig();
        config.setImplementation(getClassInstance());
        splitBrainProtectionConfig.addListenerConfig(config);
    }

    @Override
    protected void addClassNameToConfig() throws UnsupportedOperationException {
        splitBrainProtectionConfig.addListenerConfig(new SplitBrainProtectionListenerConfig(getUserDefinedClassName()));
    }

    @Override
    protected String getUserDefinedClassName() {
        return "usercodedeployment.CustomSplitBrainProtectionListener";
    }

    @Parameterized.Parameters(name = "Connection: {0}, Config: {1}, Class Registration: {2}, Assertion: {3}")
    public static Iterable<Object[]> parameters() {
        // Dynamic configuration for SplitBrainProtectionListeners is not supported
        return listenerParametersWithoutInstanceInDataStructure()
                .stream()
                .filter(obj -> obj[1] != ConfigStyle.DYNAMIC)
                .collect(Collectors.toList());
    }
}
