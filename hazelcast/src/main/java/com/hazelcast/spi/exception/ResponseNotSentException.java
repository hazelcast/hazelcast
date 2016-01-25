/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.exception;

import com.hazelcast.core.HazelcastException;

/**
 * A HazelcastException indicating that there is some kind of system error causing
 * a response NOT to be sent for some operation.
 * <p/>
 * This can happen when connection to target member is dropped for any reason,
 * either a short network disconnection or target member leaves the cluster.
 */
public class ResponseNotSentException extends HazelcastException {

    public ResponseNotSentException(String message) {
        super(message);
    }
}
