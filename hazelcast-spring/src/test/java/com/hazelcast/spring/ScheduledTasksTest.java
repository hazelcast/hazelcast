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

package com.hazelcast.spring;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"scheduled-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
public class ScheduledTasksTest {

    @Resource(name = "scheduler")
    private TaskScheduler scheduler;

    private final AtomicInteger counter = new AtomicInteger();

    @BeforeClass
    @AfterClass
    public static void start() {
        Hazelcast.shutdownAll();
    }

    @Scheduled(initialDelay = 0, fixedDelay = 100)
    public void scheduledMethod() {
        counter.incrementAndGet();
    }

    @Test
    public void testScheduledMethod_isExecuted() {
        assertInstanceOf(IScheduledExecutorService.class, scheduler);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertThat(counter.get(), greaterThan(5));
            }
        });
    }
}
