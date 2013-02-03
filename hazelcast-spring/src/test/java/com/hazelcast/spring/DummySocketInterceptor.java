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

package com.hazelcast.spring;

import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.nio.MemberSocketInterceptor;

import java.io.IOException;
import java.net.Socket;

/**
 *
 */
public class DummySocketInterceptor implements MemberSocketInterceptor {
    public void init(final SocketInterceptorConfig socketInterceptorConfig) {
    }

    public void onAccept(final Socket acceptedSocket) throws IOException {
    }

    public void onConnect(final Socket connectedSocket) throws IOException {
    }
}
