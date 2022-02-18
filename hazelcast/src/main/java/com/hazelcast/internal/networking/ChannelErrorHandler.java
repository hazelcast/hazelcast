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

/**
 * A strategy for controlling what needs to be done in case of an Exception
 * being thrown when the {@link Networking} processes events.
 *
 * For example if a connection is making use of the Channel, the Connection
 * could close itself when an error was encountered.
 */
public interface ChannelErrorHandler {

    /**
     * Called when an error was detected.
     *
     * @param channel the Channel that ran into an error. It could be that
     *                the Channel is null if error
     *                was thrown not related to a particular Channel.
     * @param error   the Throwable causing problems
     */
    void onError(Channel channel, Throwable error);
}
