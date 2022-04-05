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

import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

public class MockLoggingFactory implements LoggerFactory {

    static List<String> capturedMessages = new ArrayList<>();

    @Override
    public ILogger getLogger(String name) {
        return new AbstractLogger() {
            @Override
            public void log(Level level, String message) {
               log(level, message, null);
            }

            @Override
            public void log(Level level, String message, Throwable thrown) {
                System.out.printf("%s %s: %s%n", Util.toLocalTime(System.currentTimeMillis()), level, message);
                capturedMessages.add(message);
                if (thrown != null) {
                    thrown.printStackTrace();
                }
            }

            @Override
            public void log(com.hazelcast.logging.LogEvent logEvent) {
                log(logEvent.getLogRecord().getLevel(), logEvent.getLogRecord().getMessage());
            }

            @Override
            public Level getLevel() {
                return Level.INFO;
            }

            @Override
            public boolean isLoggable(Level level) {
                return level.intValue() >= getLevel().intValue();
            }

        };
    }
}
