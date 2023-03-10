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
import com.hazelcast.jet.impl.AbstractJetInstance;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MemberExecuteJarStrategyTest {

    @Test
    public void testInvokeMain() throws InvocationTargetException, IllegalAccessException, InterruptedException {
        // Mock HazelcastInstance and AbstractJetInstance
        HazelcastInstance hazelcastInstance = Mockito.mock(HazelcastInstance.class);
        AbstractJetInstance abstractJetInstance = Mockito.mock(AbstractJetInstance.class);
        Mockito.when(hazelcastInstance.getJet()).thenReturn(abstractJetInstance);
        Mockito.when(abstractJetInstance.getHazelcastInstance()).thenReturn(hazelcastInstance);

        // Mock main method
        Method method = Mockito.mock(Method.class);

        // Create BootstrappedInstanceProxy
        BootstrappedInstanceProxy instanceProxy = BootstrappedInstanceProxy.createWithJetProxy(hazelcastInstance);


        MemberExecuteJarStrategy memberExecuteJarStrategy = new MemberExecuteJarStrategy();

        ThreadLocal<Integer> threadLocal = new ThreadLocal<>();

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int index = 0; index < 5; index++) {
            final int value = index;
            executorService.submit(() -> {
                threadLocal.set(value);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println(" = " + threadLocal.get());
            });
        }


        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);

//        String jarPath = "jarPath";
//        String snapshotName = "snapshotName";
//        String jobName = "jobName";
//
//        ExecuteJobParameters executeJobParameters = new ExecuteJobParameters()
//
//        memberExecuteJarStrategy.invokeMain(instanceProxy,
//                jarPath,
//                snapshotName,
//                jobName,
//                method,
//                new String[]{"jobArgs"}
//        );
//
//        BootstrappedJetProxy bootstrappedJetProxy = instanceProxy.getJet();
//        assertThat(bootstrappedJetProxy.getJar()).isEqualTo(jarPath);
//        assertThat(bootstrappedJetProxy.getSnapshotName()).isEqualTo(snapshotName);
//        assertThat(bootstrappedJetProxy.getJobName()).isEqualTo(jobName);
    }
}
