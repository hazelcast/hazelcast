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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Test that security banners are logged on proper levels.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class NodeExtensionSecurityBannerTest extends HazelcastTestSupport {

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-debug.xml");

    @Test
    public void testTwoMessagesPrinted() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = context.getConfiguration();
        TestAppender appender = new TestAppender();
        appender.start();
        configuration.addAppender(appender);
        configuration.getRootLogger().addAppender(appender, null, null);
        createHazelcastInstance();
        assertEquals(2, appender.events.size());
        assertEquals(Level.INFO, appender.events.get(0).getLevel());
        LogEvent secondLog = appender.events.get(1);
        assertEquals(Level.DEBUG, secondLog.getLevel());
        String secondMsg = secondLog.getMessage().getFormattedMessage();
        assertTrue(secondMsg.contains("⚠️ Use a custom cluster name"));
        assertTrue(secondMsg.contains("✅ Disable Jet resource upload"));
    }

    private static class TestAppender extends AbstractAppender {

        final List<LogEvent> events = new CopyOnWriteArrayList<>();

        TestAppender() {
            super(TestAppender.class.getName(), null, null, true, null);
        }

        @Override
        public void append(LogEvent event) {
            if ("com.hazelcast.system.security".equals(event.getLoggerName())) {
                events.add(event);
            }
        }

    }
}
