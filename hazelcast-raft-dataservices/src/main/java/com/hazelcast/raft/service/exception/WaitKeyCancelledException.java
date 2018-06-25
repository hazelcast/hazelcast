/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.exception;

import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.core.HazelcastException;

/**
 * Thrown when a wait key is cancelled and means that the corresponding operation has not succeeded
 */
public class WaitKeyCancelledException extends HazelcastException {

    // TODO [basri] fixit
    public static final int ERROR_CODE = 6768;


    public WaitKeyCancelledException() {
    }

    public WaitKeyCancelledException(String message) {
        super(message);
    }

    public WaitKeyCancelledException(String message, Throwable cause) {
        super(message, cause);
    }

    public static void register(ClientExceptionFactory factory) {
        factory.register(ERROR_CODE, WaitKeyCancelledException.class, new ClientExceptionFactory.ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new WaitKeyCancelledException(message, cause);
            }
        });
    }

}
