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

public class RestCallExecutionListener {

    private final RestCallCollector collector;

    private final RestCallExecution executionDetails = new RestCallExecution();

    public RestCallExecutionListener(RestCallCollector collector) {
        this.collector = collector;
    }

    void requestPathDetermined(String requestPath) {
        executionDetails.setRequestPath(requestPath);
    }

    void objectTypeDetermined(RestCallExecution.ObjectType objectType) {
        executionDetails.setObjectType(objectType);
    }

    void objectNameDetermined(String objectName) {
        executionDetails.setObjectName(objectName);
    }

    void httpMethodDetermined(RestCallExecution.HttpMethod method) {
        executionDetails.setMethod(method);
    }

    void responseSent(int statusCode) {
        int existingStatusCode = executionDetails.getStatusCode();
        if (existingStatusCode > 0) {
            throw new IllegalStateException("can not set statusCode to " + statusCode + ", it is already " + existingStatusCode);
        }
        executionDetails.setStatusCode(statusCode);
        collector.collectExecution(executionDetails);
    }
}
