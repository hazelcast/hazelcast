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

package com.hazelcast.internal.tpc.nio;

import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.AsyncServerSocketTest;
import com.hazelcast.internal.tpc.Eventloop;


public class NioAsyncServerSocketTest extends AsyncServerSocketTest {
    @Override
    public Eventloop createEventloop() {
        NioEventloop eventloop = new NioEventloop();
        loops.add(eventloop);
        eventloop.start();
        return eventloop;
    }

    @Override
    public AsyncServerSocket createAsyncServerSocket(Eventloop eventloop) {
        NioAsyncServerSocket socket = NioAsyncServerSocket.open((NioEventloop) eventloop);
        closeables.add(socket);
        return socket;
    }
}
