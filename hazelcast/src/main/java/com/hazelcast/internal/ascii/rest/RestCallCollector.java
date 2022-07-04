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

package com.hazelcast.internal.ascii.rest;

import com.hazelcast.internal.ascii.TextCommandConstants;
import com.hazelcast.internal.util.counters.MwCounter;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.ascii.rest.HttpCommandProcessor.URI_CONFIG_RELOAD;
import static com.hazelcast.internal.ascii.rest.HttpCommandProcessor.URI_CONFIG_UPDATE;

public class RestCallCollector {

    public static final int SET_SIZE_LIMIT = 1000;

    private static class RequestIdentifier {

        private final TextCommandConstants.TextCommandType method;
        private final String path;

        RequestIdentifier(TextCommandConstants.TextCommandType method, String path) {
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

    private final MwCounter configUpdateSuccCount = MwCounter.newMwCounter();
    private final MwCounter configUpdateFailCount = MwCounter.newMwCounter();;
    private final MwCounter configReloadSuccCount = MwCounter.newMwCounter();
    private final MwCounter configReloadFailCount = MwCounter.newMwCounter();

    private final MwCounter requestCount = MwCounter.newMwCounter();
    private final ConcurrentHashMap.KeySetView<RequestIdentifier, Boolean> uniqueRequests = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap.KeySetView<String, Boolean> accessedMaps = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap.KeySetView<String, Boolean> accessedQueues = ConcurrentHashMap.newKeySet();

    void collectExecution(HttpCommand command) {
        RestCallExecution execution = command.getExecutionDetails();
        TextCommandConstants.TextCommandType type = command.getType();
        updateRequestCounters(type, execution);
        String objectName = execution.getObjectName();
        updateAccessedObjectSets(execution, objectName);
        switch (type) {
            case HTTP_POST:
                handlePost(execution);
                break;
            case HTTP_GET:
                handleGet(execution);
                break;
            case HTTP_DELETE:
                handleDelete(execution);
                break;
            default:
                // no-op
        }
    }

    private void handleDelete(RestCallExecution execution) {
        RestCallExecution.ObjectType objectType = execution.getObjectType();
        if (objectType == null) {
            return;
        }
        switch (objectType) {
            case MAP:
                (execution.isSuccess() ? mapDeleteSuccCount : mapDeleteFailCount).inc();
                break;
            case QUEUE:
                (execution.isSuccess() ? queueDeleteSuccCount : queueDeleteFailCount).inc();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + objectType);
        }
    }

    private void handleGet(RestCallExecution execution) {
        RestCallExecution.ObjectType objectType = execution.getObjectType();
        if (objectType == null) {
            return;
        }
        switch (objectType) {
            case MAP:
                (execution.isSuccess() ? mapGetSuccCount : mapGetFailCount).inc();
                break;
            case QUEUE:
                (execution.isSuccess() ? queueGetSuccCount : queueGetFailCount).inc();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + execution.getObjectType());
        }
    }

    private void handlePost(RestCallExecution execution) {
        RestCallExecution.ObjectType objectType = execution.getObjectType();
        if (objectType == null) {
            if (execution.getRequestPath().endsWith(URI_CONFIG_UPDATE)) {
                (execution.isSuccess() ? configUpdateSuccCount : configUpdateFailCount).inc();
            }  else if (execution.getRequestPath().endsWith(URI_CONFIG_RELOAD)) {
                (execution.isSuccess() ? configReloadSuccCount : configReloadFailCount).inc();
            }
            return;
        }
        switch (objectType) {
            case MAP:
                (execution.isSuccess() ? mapPostSuccCount : mapPostFailCount).inc();
                break;
            case QUEUE:
                (execution.isSuccess() ? queuePostSuccCount : queuePostFailCount).inc();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + execution.getObjectType());
        }
    }

    private void updateAccessedObjectSets(RestCallExecution execution, String objectName) {
        if (objectName == null) {
            return;
        }
        RestCallExecution.ObjectType objectType = execution.getObjectType();
        if (objectType == null) {
            return;
        }
        switch (objectType) {
            case MAP:
                accessedMaps.add(objectName);
                break;
            case QUEUE:
                accessedQueues.add(objectName);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + objectType);
        }
    }

    private void updateRequestCounters(TextCommandConstants.TextCommandType type, RestCallExecution execution) {
        if (uniqueRequests.size() < SET_SIZE_LIMIT) {
            uniqueRequests.add(new RequestIdentifier(type, execution.getRequestPath()));
        }
        requestCount.inc();
        RestCallExecution.ObjectType objectType = execution.getObjectType();
        if (objectType == null) {
            return;
        }
        switch (objectType) {
            case MAP:
                mapTotalRequestCount.inc();
                break;
            case QUEUE:
                queueTotalRequestCount.inc();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + objectType);
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

    public String getQueueGetSuccessCount() {
        return String.valueOf(queueGetSuccCount.get());
    }

    public String getQueueGetFailureCount() {
        return String.valueOf(queueGetFailCount.get());
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
        return String.valueOf(mapDeleteFailCount.get());
    }

    public String getTotalMapRequestCount() {
        return String.valueOf(mapTotalRequestCount.get());
    }

    public String getConfigUpdateSuccessCount() {
        return String.valueOf(configUpdateSuccCount.get());
    }

    public String getConfigUpdateFailureCount() {
        return String.valueOf(configUpdateFailCount.get());
    }

    public String getConfigReloadSuccessCount() {
        return String.valueOf(configReloadSuccCount.get());
    }

    public String getConfigReloadFailureCount() {
        return String.valueOf(configReloadFailCount.get());
    }

    public String getTotalQueueRequestCount() {
        return String.valueOf(queueTotalRequestCount.get());
    }
}
