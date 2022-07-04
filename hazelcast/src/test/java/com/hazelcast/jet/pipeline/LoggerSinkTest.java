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

package com.hazelcast.jet.pipeline;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LoggerSinkTest extends JetTestSupport {

    private static final String HAZELCAST_LOGGING_TYPE = "hazelcast.logging.type";
    private static final String HAZELCAST_LOGGING_CLASS = "hazelcast.logging.class";

    private String prevLoggingType;
    private String prevLoggingClass;

    @Before
    public void setup() {
        prevLoggingType = System.getProperty(HAZELCAST_LOGGING_TYPE);
        prevLoggingClass = System.getProperty(HAZELCAST_LOGGING_CLASS);

        System.clearProperty(HAZELCAST_LOGGING_TYPE);
        System.setProperty(HAZELCAST_LOGGING_CLASS, MockLoggingFactory.class.getCanonicalName());
    }

    @Test
    public void loggerSink() {
        // Given
        HazelcastInstance hz = createHazelcastInstance();
        String srcName = randomName();

        hz.getList(srcName).add(0);

        Pipeline p = Pipeline.create();

        // When
        p.readFrom(Sources.<Integer>list(srcName))
         .map(i -> i + "-shouldBeSeenOnTheSystemOutput")
         .writeTo(Sinks.logger());

        hz.getJet().newJob(p).join();

        // Then
        Assert.assertTrue("no message containing '0-shouldBeSeenOnTheSystemOutput' was found",
                MockLoggingFactory.capturedMessages
                        .stream()
                        .anyMatch(message -> message.contains("0-shouldBeSeenOnTheSystemOutput"))
        );
    }

    @After
    public void after() {
        if (prevLoggingType == null) {
            System.clearProperty(HAZELCAST_LOGGING_TYPE);
        } else {
            System.setProperty(HAZELCAST_LOGGING_TYPE, prevLoggingType);
        }
        if (prevLoggingClass == null) {
            System.clearProperty(HAZELCAST_LOGGING_CLASS);
        } else {
            System.setProperty(HAZELCAST_LOGGING_CLASS, prevLoggingClass);
        }
    }

}
