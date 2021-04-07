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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class JetClassLoaderTest extends JetTestSupport {

    @Test
    public void when_jobCompleted_then_classLoaderShutDown() {
        DAG dag = new DAG();
        dag.newVertex("v", LeakClassLoaderP::new).localParallelism(1);

        JetInstance instance = createJetMember();

        // When
        instance.newJob(dag).join();

        // Then
        assertTrue("The classloader should have been shutdown after job completion",
                LeakClassLoaderP.classLoader.isShutdown()
        );
    }

    private static class LeakClassLoaderP extends AbstractProcessor {

        private static volatile JetClassLoader classLoader;

        @Override
        public boolean complete() {
            classLoader = (JetClassLoader) Thread.currentThread().getContextClassLoader();
            return true;
        }
    }
}
