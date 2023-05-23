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

import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorBuilder;
import com.hazelcast.internal.tpcengine.net.AcceptRequest;
import com.hazelcast.internal.tpcengine.net.AsyncSocketBuilder;
import com.hazelcast.internal.tpcengine.net.AsyncSocketTest;
import com.hazelcast.internal.tpcengine.net.DefaultSSLEngineFactory;

import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SSL_ENGINE_FACTORY;

public class TlsNioAsyncSocketTest extends AsyncSocketTest {

    @Override
    public ReactorBuilder newReactorBuilder() {
        return new NioReactorBuilder();
    }

    @Override
    protected AsyncSocketBuilder newAsyncSocketBuilder(Reactor reactor) {
        return reactor.newAsyncSocketBuilder()
                .set(SSL_ENGINE_FACTORY, new DefaultSSLEngineFactory());
    }

    @Override
    protected AsyncSocketBuilder newAsyncSocketBuilder(Reactor reactor, AcceptRequest acceptRequest) {
        return reactor.newAsyncSocketBuilder(acceptRequest)
                .set(SSL_ENGINE_FACTORY, new DefaultSSLEngineFactory());
    }
}
