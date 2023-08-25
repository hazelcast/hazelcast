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

package com.hazelcast.internal.tpcengine.net;

import java.util.function.Consumer;

/**
 * Contains an accept request when a socket connects to the {@link AsyncServerSocket}. Is
 * processed by setting the {@link AsyncServerSocketBuilder#setAcceptConsumer(Consumer)}.
 * <p/>
 * Currently it is just a dumb placeholder so that we can pass the appropriate resource
 * (e.g. the accepted SocketChannel) to the constructor of the AsyncSocket in a typesafe
 * manner.
 */
public interface AcceptRequest extends AutoCloseable {

}
