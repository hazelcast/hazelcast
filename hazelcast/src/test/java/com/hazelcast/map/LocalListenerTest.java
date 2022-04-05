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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static com.hazelcast.map.impl.event.MapEventPublisherImpl.PROP_LISTENER_WITH_PREDICATE_PRODUCES_NATURAL_EVENT_TYPES;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.starter.ReflectionUtils.setFieldValueReflectively;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalListenerTest extends HazelcastTestSupport {

    @Test
    public void no_exception_when_not_notifiable_listener() throws IllegalAccessException {
        Config config = getConfig()
                .setProperty(PROP_LISTENER_WITH_PREDICATE_PRODUCES_NATURAL_EVENT_TYPES, "true");

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<String, String> map = instance.getMap(randomString());

        MapEventPublisherLogger mapEventPublisherLogger = new MapEventPublisherLogger();
        injectLogger(instance, mapEventPublisherLogger);

        // this entry-removed-listener is not notifiable,
        // since we expect entry added events.
        map.addLocalEntryListener((EntryRemovedListener<String, String>) event -> {
        });

        // generate entry-added event
        map.put("key", "value");

        // no exception we expect (we use assertTrueAllTheTime
        // since event is fired after put return)
        assertTrueAllTheTime(() -> assertTrue(mapEventPublisherLogger.logCollector.toString(),
                mapEventPublisherLogger.logCollector.isEmpty()), 5);

    }

    private void injectLogger(HazelcastInstance instance,
                              MapEventPublisherLogger mapEventPublisherLogger) throws IllegalAccessException {
        NodeEngineImpl nodeEngine1 = getNodeEngineImpl(instance);
        MapService mapService = nodeEngine1.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
        setFieldValueReflectively(mapEventPublisher, "logger", mapEventPublisherLogger);
    }

    private class MapEventPublisherLogger extends AbstractLogger {

        private final CopyOnWriteArrayList logCollector
                = new CopyOnWriteArrayList<>();

        @Override
        public void log(Level level, String message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            logCollector.add(level);
            logCollector.add(message);
            logCollector.add(thrown);
        }

        @Override
        public void log(LogEvent logEvent) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Level getLevel() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isLoggable(Level level) {
            return false;
        }
    }
}
