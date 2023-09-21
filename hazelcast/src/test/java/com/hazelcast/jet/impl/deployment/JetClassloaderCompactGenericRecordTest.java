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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.config.Config;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.Repeat;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.ThreadLocalRandom;


@Category({NightlyTest.class, ParallelJVMTest.class})
public class JetClassloaderCompactGenericRecordTest extends SimpleTestInClusterSupport {

    public static final String MAP_NAME = "various_compact";
    // test is very fast unless it hits a deadlock,
    // deadlock also causes instance shutdown to timeout.
    @Rule
    public final Timeout timeoutRule = Timeout.seconds(30);

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        initializeWithClient(1, config, null);
    }

    @Repeat(1000)
    @Test(timeout = 15_000)
    public void whenCompactGenericRecordInImap_thenShouldNotDeadlock() throws Throwable {
        // prepare IMap
        IMap<Object, Object> map = instance().getMap(MAP_NAME);
        map.clear();
        for (int i = 0; i < 100; ++i) {
            // randomness increases likelihood of deadlock
            map.put("key" + ThreadLocalRandom.current().nextInt(),
                    GenericRecordBuilder.compact("key" + ThreadLocalRandom.current().nextInt())
                            .setString("hello", "world" + i)
                            .build());
        }

        // prepare Jet job with custom classes so JetClassLoader is created.
        // The class does not have to be used in the job.
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.map(MAP_NAME))
                // logger triggers deserialization of Compact GenericRecord
                .writeTo(Sinks.logger())
                .getPipeline();

        JobConfig jobConfig = new JobConfig();
        URL classUrl = new File(AbstractDeploymentTest.CLASS_DIRECTORY).toURI().toURL();
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{classUrl}, null);
        Class<?> appearance = urlClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
        jobConfig.addClass(appearance);

        // prepare entry processor
        Thread asyncExecuteOnEntries = new Thread(() ->
                map.executeOnEntries(e -> {
                    // Give some time for Jet job to start.
                    // We want this entry processor to be executed in parallel with Jet job execution.
                    sleepMillis(1);
                    return e.getValue().toString();
                }));

        // execute in parallel
        asyncExecuteOnEntries.start();
        client().getJet().newJob(pipeline, jobConfig).join();
        asyncExecuteOnEntries.join();
    }
}
