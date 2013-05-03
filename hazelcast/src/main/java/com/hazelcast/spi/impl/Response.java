/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

/**
 * @mdogan 4/10/13
 */
class Response {

    final Object response;

    final long callId;

    final int backupCount;

    Response(Object response, long callId, int backupCount) {
        this.response = response;
        this.callId = callId;
        this.backupCount = backupCount;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Response");
        sb.append("{response=").append(response);
        sb.append(", callId=").append(callId);
        sb.append(", backupCount=").append(backupCount);
        sb.append('}');
        return sb.toString();
    }
}
