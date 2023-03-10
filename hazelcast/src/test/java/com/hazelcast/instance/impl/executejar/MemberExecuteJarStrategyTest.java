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

package com.hazelcast.instance.impl.executejar;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.BootstrappedInstanceProxy;
import com.hazelcast.instance.impl.BootstrappedJetProxy;
import com.hazelcast.jet.JetService;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class MemberExecuteJarStrategyTest {

    @Test
    public void testInvokeMain() throws InvocationTargetException, IllegalAccessException {
        // Mock HazelcastInstance and JetService
        HazelcastInstance hazelcastInstance = Mockito.mock(HazelcastInstance.class);
        JetService abstractJetInstance = Mockito.mock(JetService.class);
        Mockito.when(hazelcastInstance.getJet()).thenReturn(abstractJetInstance);

        // Mock main method
        Method method = Mockito.mock(Method.class);

        BootstrappedInstanceProxy instanceProxy = BootstrappedInstanceProxy.createWithJetProxy(hazelcastInstance);

        MemberExecuteJarStrategy memberExecuteJarStrategy = new MemberExecuteJarStrategy();

        String jarPath = "jarPath";
        String snapshotName = "snapshotName";
        String jobName = "jobName";

        ExecuteJobParameters executeJobParameters = new ExecuteJobParameters(jarPath, snapshotName, jobName);

        // Test that invokeMain sets thread local values in BootstrappedInstanceProxy
        memberExecuteJarStrategy.invokeMain(instanceProxy,
                executeJobParameters,
                method,
                Collections.singletonList("jobArgs")
        );

        BootstrappedJetProxy bootstrappedJetProxy = instanceProxy.getJet();
        ExecuteJobParameters parameters = bootstrappedJetProxy.getExecuteJobParameters();

        assertThat(parameters.getJarPath()).isEqualTo(jarPath);
        assertThat(parameters.getSnapshotName()).isEqualTo(snapshotName);
        assertThat(parameters.getJobName()).isEqualTo(jobName);
    }
}
