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

package com.hazelcast.instance.impl;

import static com.hazelcast.instance.impl.Node.SECURITY_BANNER_CATEGORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Test that security banners are logged on proper levels.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class NodeExtensionSecurityBannerTest extends HazelcastTestSupport {

    @Rule
    public OverridePropertyRule sysPropSecurityBanner = OverridePropertyRule.set(SECURITY_BANNER_CATEGORY, null);

    private LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);

    @After
    public void after() {
        context.setConfigLocation(null);
    }

    @Test
    public void testDebugLevel() throws Exception {
        context.setConfigLocation(getClass().getClassLoader().getResource("log4j2-debug.xml").toURI());
        TestAppender appender = configureTestAppender();
        createHazelcastInstance();
        assertEquals(2, appender.events.size());
        assertEquals(Level.INFO, appender.events.get(0).getLevel());
        LogEvent secondLog = appender.events.get(1);
        assertEquals(Level.DEBUG, secondLog.getLevel());
        assertRecommendationContent(secondLog);
    }

    @Test
    public void testSystemProperty() throws Exception {
        sysPropSecurityBanner.setOrClearProperty("");
        TestAppender appender = configureTestAppender();
        createHazelcastInstance();
        assertEquals(2, appender.events.size());
        assertEquals(Level.INFO, appender.events.get(0).getLevel());
        LogEvent secondLog = appender.events.get(1);
        assertEquals(Level.INFO, secondLog.getLevel());
        assertRecommendationContent(secondLog);
    }

    @Test
    public void testDefault() throws Exception {
        Configuration configuration = context.getConfiguration();
        TestAppender appender = new TestAppender();
        appender.start();
        configuration.addAppender(appender);
        configuration.getRootLogger().addAppender(appender, null, null);
        createHazelcastInstance();
        assertEquals(1, appender.events.size());
        assertEquals(Level.INFO, appender.events.get(0).getLevel());
    }

    private void assertRecommendationContent(LogEvent logEvent) {
        String secondMsg = logEvent.getMessage().getFormattedMessage();
        assertTrue(secondMsg.contains("⚠️ Use a custom cluster name"));
        assertTrue(secondMsg.contains("✅ Disable member multicast"));
    }

    private TestAppender configureTestAppender() {
        Configuration configuration = context.getConfiguration();
        TestAppender appender = new TestAppender();
        appender.start();
        configuration.addAppender(appender);
        configuration.getRootLogger().addAppender(appender, null, null);
        return appender;
    }

    private static class TestAppender extends AbstractAppender {

        final List<LogEvent> events = new CopyOnWriteArrayList<>();

        TestAppender() {
            super(TestAppender.class.getName(), null, null, true, null);
        }

        @Override
        public void append(LogEvent event) {
            if (SECURITY_BANNER_CATEGORY.equals(event.getLoggerName()) && events.size() < 10) {
                events.add(event);
            }
        }
    }
}
