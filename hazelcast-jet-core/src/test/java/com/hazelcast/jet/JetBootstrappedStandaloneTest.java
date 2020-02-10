/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.test.SerialTest;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(SerialTest.class)
public class JetBootstrappedStandaloneTest extends JetTestSupport {

    private static JetInstance bootstrappedInstance;

    @BeforeClass
    public static void initializeBootstrappedInstance() {
        bootstrappedInstance = Jet.bootstrappedInstance();
    }

    @AfterClass
    public static void shutdownBootstrappedInstance() {
        if (bootstrappedInstance != null) {
            bootstrappedInstance.shutdown();
        }
    }

    @Test
    public void bootstrappedInstanceIsStartedAsStandalone() {
        // start different cluster
        createJetMember();

        int size = bootstrappedInstance.getHazelcastInstance().getCluster().getMembers().size();
        assertEquals(1, size);
    }

    @Test
    public void bootstrappedInstanceCanExecuteJob() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(0, 1, 2, 3, 4))
                .writeTo(Sinks.list("sinkList"));

        bootstrappedInstance.newJob(p).join();

        List<Integer> sinkList = bootstrappedInstance.getList("sinkList");
        assertEquals(5, sinkList.size());
    }

    @Test
    public void moreBootstrappedInstanceCallReturnsTheSameInstance() {
        JetInstance bootstrappedInstance2 = Jet.bootstrappedInstance();
        assertEquals(bootstrappedInstance, bootstrappedInstance2);
    }
}
