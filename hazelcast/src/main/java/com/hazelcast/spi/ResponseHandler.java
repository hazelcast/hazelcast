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

package com.hazelcast.spi;

/**
 * A handler for the {@link com.hazelcast.spi.OperationService} when it has calculated a response. This way you can hook
 * into the system and decide what to do with it, e.g. send it to the right machine,
 */
public interface ResponseHandler {

    /**
     * Sends a response. The backupCount defaults to 0.
     *
     * @param obj the response.
     */
    void sendResponse(Object obj);

    /**
     * Sends a response.
     *
     * @param obj the response content
     * @param backupCount the number of backups the invoker needs to wait for.
     */
    void sendResponse(Object obj, int backupCount);

    /**
     * Send a call timeout. So when an invocation needs to be notified of a timeout.
     */
    void sendCallTimeout();

    /**
     * Checks if the invocation is a local invocation.
     *
     * @return true if local, false otherwise.
     */
    boolean isLocal();
}
