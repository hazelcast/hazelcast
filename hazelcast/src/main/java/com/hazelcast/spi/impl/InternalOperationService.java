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

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

/**
 * This is the interface that needs to be implemented by actual InternalOperationService. Currently there is a single
 * InternalOperationService: {@link com.hazelcast.spi.impl.BasicOperationService}, but in the future others can be added.
 * <p/>
 * It exposes methods that will not be called by regular code, like shutdown, but will only be called by
 * the the SPI management.
 */
public interface InternalOperationService extends OperationService {

     void onMemberLeft(MemberImpl member);

    boolean isCallTimedOut(Operation op);

    void notifyBackupCall(long callId);

    /**
     * Shuts down this InternalOperationService.
     */
    void shutdown();
}
