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
 * The possible actions that can be taken when a certain exception is thrown. E.g. when a map.get is executed on a member
 * where the partition has just moved to another box, the ExceptionAction.RETRY_INVOCATION would be used.
 */
public enum ExceptionAction {

    /**
     * Indicates that the operation can be retried.
     */
    RETRY_INVOCATION,

    /**
     * Indicates that more waiting can be done (e.g. a Condition.await)
     */
    CONTINUE_WAIT,

    /**
     * Indicates that the exception should be bubble up.
     */
    THROW_EXCEPTION
}
