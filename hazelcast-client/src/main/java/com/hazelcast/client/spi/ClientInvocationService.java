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

package com.hazelcast.client.spi;

import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.Address;

/**
 * @author mdogan 5/16/13
 */
public interface ClientInvocationService {

    <T> ICompletableFuture<T> invokeOnRandomTarget(ClientRequest request) throws Exception;

    <T> ICompletableFuture<T> invokeOnTarget(ClientRequest request, Address target) throws Exception;

    <T> ICompletableFuture<T> invokeOnKeyOwner(ClientRequest request, Object key) throws Exception;

    <T> ICompletableFuture<T> invokeOnRandomTarget(ClientRequest request, EventHandler handler) throws Exception;

    <T> ICompletableFuture<T> invokeOnTarget(ClientRequest request, Address target, EventHandler handler) throws Exception;

    <T> ICompletableFuture<T> invokeOnKeyOwner(ClientRequest request, Object key, EventHandler handler) throws Exception;

}
