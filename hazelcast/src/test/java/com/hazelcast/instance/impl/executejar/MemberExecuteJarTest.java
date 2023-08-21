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
import com.hazelcast.instance.impl.executejar.instancedecorator.BootstrappedInstanceDecorator;
import com.hazelcast.instance.impl.executejar.jetservicedecorator.BootstrappedJetServiceDecorator;
import com.hazelcast.jet.impl.AbstractJetInstance;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;

import static com.hazelcast.instance.impl.executejar.instancedecorator.BootstrappedInstanceDecoratorFactory.createWithMemberJetProxy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MemberExecuteJarTest {

    // Empty method for testing because Method.class can not be mocked
    public static void main(String[] args) {
    }

    @Test
    public void testInvokeMain() throws InvocationTargetException, IllegalAccessException {
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        AbstractJetInstance abstractJetInstance = mock(AbstractJetInstance.class);
        when(hazelcastInstance.getJet()).thenReturn(abstractJetInstance);
        when(abstractJetInstance.getHazelcastInstance()).thenReturn(hazelcastInstance);

        // Parameter for test
        BootstrappedInstanceDecorator instanceDecorator = createWithMemberJetProxy(hazelcastInstance);

        // Parameter for test. Empty main method
        MainMethodFinder mainMethodFinder = new MainMethodFinder();
        mainMethodFinder.getMainMethodOfClass(MemberExecuteJarTest.class);

        // Parameter for test
        String jarPath = "jarPath";
        String snapshotName = "snapshotName";
        String jobName = "jobName";

        ExecuteJobParameters executeJobParameters = new ExecuteJobParameters(jarPath, snapshotName, jobName);

        // Test that invokeMain removes thread local values in BootstrappedInstanceProxy
        MemberExecuteJar memberExecuteJar = new MemberExecuteJar();
        memberExecuteJar.invokeMain(instanceDecorator,
                executeJobParameters,
                mainMethodFinder.mainMethod,
                Collections.singletonList("jobArgs")
        );

        BootstrappedJetServiceDecorator bootstrappedJetServiceDecorator = instanceDecorator.getJet();
        ExecuteJobParameters parameters = bootstrappedJetServiceDecorator.getExecuteJobParameters();

        assertThat(parameters.getJarPath()).isNull();
        assertThat(parameters.getSnapshotName()).isNull();
        assertThat(parameters.getJobName()).isNull();
    }
}
