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

package com.hazelcast.internal.ascii.rest;

import com.hazelcast.internal.util.counters.MwCounter;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;

public class RestCallCollector {

    private static class RequestIdentifier implements Comparable<RequestIdentifier> {

        public static final Comparator<RequestIdentifier> COMPARATOR = Comparator.comparing(RequestIdentifier::getMethod)
                .thenComparing(RequestIdentifier::getPath);

        private final String method;
        private final String path;

        public RequestIdentifier(String method, String path) {
            this.method = method;
            this.path = path;
        }

        @Override
        public int compareTo(@NotNull RestCallCollector.RequestIdentifier o) {
            return COMPARATOR.compare(this, o);
        }

        public String getMethod() {
            return method;
        }

        public String getPath() {
            return path;
        }
    }

    private final MwCounter mapPostSuccCount = MwCounter.newMwCounter();
    private final MwCounter mapPostFailCount = MwCounter.newMwCounter();
    private final MwCounter mapGetSuccCount = MwCounter.newMwCounter();
    private final MwCounter mapGetFailCount = MwCounter.newMwCounter();
    private final MwCounter queuePostSuccCount = MwCounter.newMwCounter();
    private final MwCounter queuePostFailCount = MwCounter.newMwCounter();
    private final MwCounter queueGetSuccCount = MwCounter.newMwCounter();
    private final MwCounter queueGetFailCount = MwCounter.newMwCounter();

    private final MwCounter requestCount = MwCounter.newMwCounter();
    private final ConcurrentSkipListSet<RequestIdentifier> uniqueRequests = new ConcurrentSkipListSet<>();
    private final ConcurrentSkipListSet<String> accessedMaps = new ConcurrentSkipListSet<>();
    private final ConcurrentSkipListSet<String> accessedQueues = new ConcurrentSkipListSet<>();

    public void collectExecution(RestCallExecution execution) {
        if (uniqueRequests.size() < 1000) {
            uniqueRequests.add(new RequestIdentifier(execution.getMethod(), execution.getRequestPath()));
        }
        requestCount.inc();
        boolean isMap = "map".equals(execution.getObjectType());
        boolean isQueue = "queue".equals(execution.getObjectType());
        String objectName = execution.getObjectName();
        if (objectName != null) {
            if (isMap) {
                accessedMaps.add(objectName);
            } else if (isQueue) {
                accessedQueues.add(objectName);
            }
        }
        if (execution.getMethod().equalsIgnoreCase("post")) {
            if (isMap) {
                (execution.isSuccess() ? mapPostSuccCount : mapPostFailCount).inc();
            } else if (isQueue) {
                (execution.isSuccess() ? queuePostSuccCount : queuePostFailCount).inc();
            }
        } else if (execution.getMethod().equalsIgnoreCase("get")) {
            if (isMap) {
                (execution.isSuccess() ? mapGetSuccCount : mapGetFailCount).inc();
            } else if (isQueue) {
                (execution.isSuccess() ? queueGetSuccCount : queueGetFailCount).inc();
            }
        }
    }

    public String getMapPostSuccessCount() {
        return String.valueOf(mapPostSuccCount.get());
    }

    public String getMapPostFailureCount() {
        return String.valueOf(mapPostFailCount.get());
    }

    public String getRequestCount() {
        return String.valueOf(requestCount.get());
    }

    public String getUniqueRequestCount() {
        return String.valueOf(uniqueRequests.size());
    }

    public String getMapGetSuccessCount() {
        return String.valueOf(mapGetSuccCount.get());
    }

    public String getMapGetFailureCount() {
        return String.valueOf(mapGetFailCount.get());
    }

    public String getQueuePostSuccessCount() {
        return String.valueOf(queuePostSuccCount.get());
    }

    public String getQueuePostFailureCount() {
        return String.valueOf(queuePostFailCount.get());
    }

    public String getAccessedMapCount() {
        return String.valueOf(accessedMaps.size());
    }

    public String getAccessedQueueCount() {
        return String.valueOf(accessedQueues.size());
    }

}
