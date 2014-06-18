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

package com.hazelcast.nio;

import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

/**
 * An interface that provides the ability to intercept the creation of sockets.
 *
 * This class should belong in the {@link com.hazelcast.nio.tcp} package; unfortunately we can't move this
 * interface since it is a public API.
 */
public interface SocketInterceptor {

    void init(Properties properties);

    void onConnect(Socket connectedSocket) throws IOException;
}
