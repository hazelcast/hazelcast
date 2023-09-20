/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.nio;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * A handler that gets signalled when something interesting happens on a {@link SelectionKey}
 * for example data has arrived at a socket.
 */
public interface NioHandler {

    /**
     * Signals the Handler that socket should be closed.
     *
     * @param reason the reason (can be null).
     * @param cause  the cause (can be null).
     */
    void close(String reason, Throwable cause);

    /**
     * Signals that something interesting happened on a SelectionKey.
     *
     * @throws IOException if handling lead to problems.
     */
    void handle() throws IOException;
}
