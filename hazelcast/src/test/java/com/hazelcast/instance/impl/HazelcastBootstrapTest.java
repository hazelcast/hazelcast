/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastBootstrapTest {

    @AfterClass
    public static void teardown() throws NoSuchFieldException, IllegalAccessException {
        Hazelcast.bootstrappedInstance().shutdown();
        cleanUpHazelcastBootstrapSupplier();
    }

    private static void cleanUpHazelcastBootstrapSupplier() throws NoSuchFieldException, IllegalAccessException {
        // Set the static instance supplier field of HazelcastBootstrap
        // to null. Because of the lifetime of this field spans many
        // test classes run on the same JVM, HazelcastBootstrapTest
        // and HazelcastCommandLineTest were interfering with each
        // other before this cleanup step added.
        // See: https://github.com/hazelcast/hazelcast/issues/18725
        Field field = HazelcastBootstrap.class.getDeclaredField("supplier");
        field.setAccessible(true);
        field.set(null, null);
    }

    @Test
    public void testHazelcast_bootstrappedInstance() {
        HazelcastInstance hz = Hazelcast.bootstrappedInstance();
        JetService jet = hz.getJet();
        executeWithBootstrappedInstance(jet);
    }

    @Test
    public void testJet_bootstrappedInstance() {
        JetInstance jet = Jet.bootstrappedInstance();
        executeWithBootstrappedInstance(jet);
    }


    public void executeWithBootstrappedInstance(JetService jet) {
        List<Integer> expected = Arrays.asList(1, 2, 3);
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1, 2, 3))
                .writeTo(AssertionSinks.assertAnyOrder(expected));
        jet.newJob(p).join();
    }
}
