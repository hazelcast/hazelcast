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

package com.hazelcast.internal.networking;

public enum HandlerStatus {

    /**
     * The handler has processed all data it could process and there is nothing
     * left for this handler to do.
     *
     * For example a Packet was fully written and there is nothing else to write.
     *
     * Or when all data from the input buffer has been consumed and there is nothing
     * left to produce.
     */
    CLEAN,

    /**
     * There is something left in the handler that could be processed with another
     * call.
     *
     * For example a packet larger than the socket buffer needs to be written, so
     * it will require multiple calls for the handler to get this packet written.
     *
     * As long as the handler has not fully written the packet, it will keep
     * returning dirty.
     *
     */
    DIRTY,

    /**
     * The handler will not accept any new reads until some external condition
     * has been met. For example for TLS Hostname verification, a handler could
     * create a task executed on some thread that does the hostname verification
     * and as a result it will return BLOCKED to indicate that this handler will
     * not produce/consume any data. So there is no point trying.
     */
    BLOCKED
}
