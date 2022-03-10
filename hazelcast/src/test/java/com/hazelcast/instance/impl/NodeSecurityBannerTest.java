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

import static com.hazelcast.instance.impl.NodeSecurityBanner.SECURITY_BANNER_CATEGORY;
import static com.hazelcast.spi.properties.ClusterProperty.LOG_EMOJI_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.SECURITY_RECOMMENDATIONS;
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
public class NodeSecurityBannerTest extends HazelcastTestSupport {

    private static final String LOGGING_TYPE_PROP_NAME = "hazelcast.logging.type";
    private static final String LOGGING_CLASS_PROP_NAME = "hazelcast.logging.class";

    @Rule
    public OverridePropertyRule sysPropSecurityBanner = OverridePropertyRule.clear(SECURITY_RECOMMENDATIONS.getName());
    @Rule
    public OverridePropertyRule sysPropEmoji = OverridePropertyRule.clear(LOG_EMOJI_ENABLED.getName());
    @Rule
    public OverridePropertyRule disableTestLogger = OverridePropertyRule.clear(LOGGING_CLASS_PROP_NAME);
    @Rule
    public OverridePropertyRule setLog4j2Logger = OverridePropertyRule.set(LOGGING_TYPE_PROP_NAME, "log4j2");

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
        assertEquals(1, appender.events.size());
        LogEvent logEvent = appender.events.get(0);
        assertEquals(Level.DEBUG, logEvent.getLevel());
        assertRecommendationContent(logEvent, Boolean.parseBoolean(LOG_EMOJI_ENABLED.getDefaultValue()));
    }

    @Test
    public void testSystemPropertyWithoutEmojis() throws Exception {
        testSystemPropertyInternal(false);
    }

    @Test
    public void testTestWithEmojis() throws Exception {
        testSystemPropertyInternal(true);
    }

    @Test
    public void testDefault() throws Exception {
        TestAppender appender = configureTestAppender();
        createHazelcastInstance();
        assertEquals(1, appender.events.size());
        LogEvent logEvent = appender.events.get(0);
        assertEquals(Level.INFO, logEvent.getLevel());
        assertHintsToDisplayBanner(logEvent);
    }

    private void testSystemPropertyInternal(boolean emojiEnabled) {
        sysPropSecurityBanner.setOrClearProperty("");
        sysPropEmoji.setOrClearProperty(Boolean.toString(emojiEnabled));
        TestAppender appender = configureTestAppender();
        createHazelcastInstance();
        assertEquals(1, appender.events.size());
        LogEvent logEvent = appender.events.get(0);
        assertEquals(Level.INFO, logEvent.getLevel());
        assertRecommendationContent(logEvent, emojiEnabled);
    }

    private void assertRecommendationContent(LogEvent logEvent, boolean emojiExpected) {
        String msg = logEvent.getMessage().getFormattedMessage();
        String expectedMsg1 = emojiExpected ? "⚠️ Use a custom cluster name" : "[ ] Use a custom cluster name";
        String expectedMsg2 = emojiExpected ? "✅ Disable member multicast" : "[X] Disable member multicast";
        assertTrue(msg.contains(expectedMsg1));
        assertTrue(msg.contains(expectedMsg2));
    }

    private void assertHintsToDisplayBanner(LogEvent logEvent) {
        String msg = logEvent.getMessage().getFormattedMessage();
        assertTrue(msg.contains(SECURITY_BANNER_CATEGORY));
        assertTrue(msg.contains("-D" + SECURITY_RECOMMENDATIONS));
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
