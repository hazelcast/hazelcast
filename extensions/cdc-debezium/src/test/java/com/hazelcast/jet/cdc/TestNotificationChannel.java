/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.cdc;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.channels.NotificationChannel;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.awaitility.Awaitility.await;

public class TestNotificationChannel implements NotificationChannel {
    private static final Map<String, Boolean> SNAPSHOT_COMPLETED = new ConcurrentHashMap<>();

    private String uuid;

    static void waitForSnapshotEnd(final String uuid) {
        await()
                .atMost(Duration.ofMinutes(5))
                .pollDelay(Duration.ofSeconds(2))
                .until(() -> isCompletedFor(uuid));
    }

    @Override
    @SuppressWarnings("deprecation")
    public void init(CommonConnectorConfig config) {
        uuid = config.getConfig().getString("notification.TestNotificationChannel.uuid");
    }

    @Override
    public String name() {
        return "TestNotificationChannel";
    }

    @Override
    public void send(Notification notification) {
        if (notification.getAggregateType().equalsIgnoreCase("Initial Snapshot")) {
            if (notification.getType().equalsIgnoreCase("COMPLETED")) {
                SNAPSHOT_COMPLETED.put(uuid, true);
            }
        }
    }

    @Override
    public void close() {
    }

    public static boolean isCompletedFor(String uuid) {
        return SNAPSHOT_COMPLETED.getOrDefault(uuid, false);
    }
}
