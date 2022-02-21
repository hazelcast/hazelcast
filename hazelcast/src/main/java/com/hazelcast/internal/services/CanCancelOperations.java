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

package com.hazelcast.internal.services;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.annotation.PrivateApi;

/**
 * Implemented by a service that can cancel its operations.
 */
@PrivateApi
public interface CanCancelOperations {
    /**
     * Notifies this service that an operation was requested to be cancelled. The caller is not aware which
     * service an operation belongs to, therefore it may call this method with call IDs of unrelated
     * operations. In such a case this method should simply return {@code false} and the caller will proceed
     * to ask other services about it.
     * <p>
     * Returning {@code true} consumes the cancellation signal only in this round of processing an
     * Operation Control packet; the same signal will be re-sent with the next packet, until the operation is
     * removed from the Invocation Registry. Therefore it is safe to return {@code true} whenever the service
     * recognizes the call ID as one it's responsible for, whether or not it actually manages
     * to cancel the operation now.
     *
     * @param caller address of the member which sent the operation
     * @param callId call ID of the operation
     * @return {@code true} if the supplied call ID is known to this service; {@code false} otherwise.
     */
    boolean cancelOperation(Address caller, long callId);
}
