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

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.ascii.rest.RestCallExecution.ObjectType.MAP;
import static com.hazelcast.internal.ascii.rest.RestCallExecution.ObjectType.QUEUE;

public class RestCallCollector {

    public static final int SET_SIZE_LIMIT = 1000;

    private static class RequestIdentifier {

        private final RestCallExecution.HttpMethod method;
        private final String path;

        RequestIdentifier(RestCallExecution.HttpMethod method, String path) {
            this.method = method;
            this.path = path;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RequestIdentifier that = (RequestIdentifier) o;
            return method.equals(that.method) && path.equals(that.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(method, path);
        }

        public RestCallExecution.HttpMethod getMethod() {
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
    private final MwCounter mapDeleteSuccCount = MwCounter.newMwCounter();
    private final MwCounter mapDeleteFailCount = MwCounter.newMwCounter();
    private final MwCounter mapTotalRequestCount = MwCounter.newMwCounter();
    private final MwCounter queuePostSuccCount = MwCounter.newMwCounter();
    private final MwCounter queuePostFailCount = MwCounter.newMwCounter();
    private final MwCounter queueGetSuccCount = MwCounter.newMwCounter();
    private final MwCounter queueGetFailCount = MwCounter.newMwCounter();
    private final MwCounter queueDeleteSuccCount = MwCounter.newMwCounter();
    private final MwCounter queueDeleteFailCount = MwCounter.newMwCounter();
    private final MwCounter queueTotalRequestCount = MwCounter.newMwCounter();

    private final MwCounter requestCount = MwCounter.newMwCounter();
    private final ConcurrentHashMap.KeySetView<RequestIdentifier, Boolean> uniqueRequests = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap.KeySetView<String, Boolean> accessedMaps = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap.KeySetView<String, Boolean> accessedQueues = ConcurrentHashMap.newKeySet();

    void collectExecution(RestCallExecution execution) {
        updateRequestCounters(execution);
        boolean isMap = execution.getObjectType() == MAP;
        boolean isQueue = execution.getObjectType() == QUEUE;
        String objectName = execution.getObjectName();
        updateAccessedObjectSets(isMap, isQueue, objectName);
        switch (execution.getMethod()) {
            case POST:
                handlePost(execution.isSuccess(), isMap, isQueue);
                break;
            case GET:
                if (isMap) {
                    (execution.isSuccess() ? mapGetSuccCount : mapGetFailCount).inc();
                } else if (isQueue) {
                    (execution.isSuccess() ? queueGetSuccCount : queueGetFailCount).inc();
                }
                break;
            case DELETE:
                if (isMap) {
                    (execution.isSuccess() ? mapDeleteSuccCount : mapDeleteFailCount).inc();
                } else if (isQueue) {
                    (execution.isSuccess() ? queueDeleteSuccCount : queueDeleteFailCount).inc();
                }
                break;
            default:
                // no-op
        }
    }

    private void handlePost(boolean isSuccess, boolean isMap, boolean isQueue) {
        if (isMap) {
            (isSuccess ? mapPostSuccCount : mapPostFailCount).inc();
        } else if (isQueue) {
            (isSuccess ? queuePostSuccCount : queuePostFailCount).inc();
        }
    }

    private void updateAccessedObjectSets(boolean isMap, boolean isQueue, String objectName) {
        if (objectName != null) {
            if (isMap) {
                accessedMaps.add(objectName);
            } else if (isQueue) {
                accessedQueues.add(objectName);
            }
        }
    }

    private void updateRequestCounters(RestCallExecution execution) {
        if (uniqueRequests.size() < SET_SIZE_LIMIT) {
            uniqueRequests.add(new RequestIdentifier(execution.getMethod(), execution.getRequestPath()));
        }
        requestCount.inc();
        if (execution.getObjectType() == MAP) {
            mapTotalRequestCount.inc();
        } else if (execution.getObjectType() == QUEUE) {
            queueTotalRequestCount.inc();
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

    public String getQueueDeleteSuccessCount() {
        return String.valueOf(queueDeleteSuccCount.get());
    }

    public String getQueueDeleteFailureCount() {
        return String.valueOf(queueDeleteFailCount.get());
    }

    public String getAccessedMapCount() {
        return String.valueOf(accessedMaps.size());
    }

    public String getAccessedQueueCount() {
        return String.valueOf(accessedQueues.size());
    }

    public String getMapDeleteSuccessCount() {
        return String.valueOf(mapDeleteSuccCount.get());
    }

    public String getMapDeleteFailureCount() {
        return String.valueOf(mapDeleteSuccCount.get());
    }

    public String getTotalMapRequestCount() {
        return String.valueOf(mapTotalRequestCount.get());
    }

    public String getTotalQueueRequestCount() {
        return String.valueOf(queueTotalRequestCount.get());
    }
}
