/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 * into the Operation execution and decide what to do with it: for example, send it to the right machine.
 *
 * Also during the development of Hazelcast 3.6 additional methods will be added to the OperationResponseHandler for certain
 * types of responses like exceptions, backup complete etc.
 *
 * @param <O> type of the {@link Operation}
 */
public interface OperationResponseHandler<O extends Operation> {

    /**
     * Sends a response.
     *
     * @param op       the operation that got executed.
     * @param response the response of the operation that got executed.
     */
    void sendResponse(O op, Object response);

    /**
     * Checks if this OperationResponseHandler is for a local invocation.
     *
     * @return true if local, false otherwise.
     */
    boolean isLocal();
}
