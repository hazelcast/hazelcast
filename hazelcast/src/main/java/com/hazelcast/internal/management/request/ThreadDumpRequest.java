/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.request;

import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.operation.ThreadDumpOperation;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.internal.util.ExceptionUtil;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.internal.util.JsonUtil.getBoolean;

/**
 * Request for generating thread dumps.
 */
public class ThreadDumpRequest implements ConsoleRequest {

    private boolean dumpDeadlocks;

    public ThreadDumpRequest() {
    }

    public ThreadDumpRequest(boolean dumpDeadlocks) {
        this.dumpDeadlocks = dumpDeadlocks;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_GET_THREAD_DUMP;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject root) {
        final JsonObject result = new JsonObject();
        InternalCompletableFuture<Object> future = mcs.callOnThis(new ThreadDumpOperation(dumpDeadlocks));
        try {
            String threadDump = (String) future.get();
            if (threadDump != null) {
                result.add("hasDump", true);
                result.add("dump", threadDump);
            } else {
                result.add("hasDump", false);
            }
        } catch (ExecutionException e) {
            addError(result, e);
        } catch (InterruptedException e) {
            addError(result, e);
            Thread.currentThread().interrupt();
        }

        root.add("result", result);
    }

    @Override
    public void fromJson(JsonObject json) {
        dumpDeadlocks = getBoolean(json, "dumpDeadlocks", false);
    }

    private static void addError(JsonObject root, Exception e) {
        root.add("hasDump", false);
        root.add("error", ExceptionUtil.toString(e));
    }
}
