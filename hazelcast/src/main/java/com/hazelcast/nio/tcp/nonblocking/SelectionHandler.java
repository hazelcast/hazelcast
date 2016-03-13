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

package com.hazelcast.nio.tcp.nonblocking;

/**
 * The SelectionHandler is a callback interface that gets called by an IO-thread when there data available to read, or space
 * available to write.
 */
public interface SelectionHandler {

    /**
     * Called when there are bytes available for reading, or space available to write.
     *
     * Any exception that leads to a termination of the connection like an IOException should not be handled in the handle method
     * but should be propagated. The reason behind this is that the handle logic already is complicated enough and by pulling it
     * out the flow will be easier to understand.
     *
     * @throws Exception
     */
    void handle() throws Exception;

    /**
     * Is called when the {@link #handle()} throws an exception.
     *
     * The idiom to use a handler is:
     * <code>
     *     try{
     *         handler.handle();
     *     } catch(Throwable t) {
     *         handler.onFailure(t);
     *     }
     * </code>
     *
     * @param throwable
     */
    void onFailure(Throwable throwable);
}
