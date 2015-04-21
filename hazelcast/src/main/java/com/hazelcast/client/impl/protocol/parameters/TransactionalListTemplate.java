/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.annotation.EncodeMethod;
import com.hazelcast.annotation.GenerateParameters;
import com.hazelcast.nio.serialization.Data;

@GenerateParameters(id = 18, name = "TransactionalList", ns = "Hazelcast.Client.Protocol.TransactionalList")
public interface TransactionalListTemplate {

    @EncodeMethod(id = 1)
    void add(String name, String txnId, long threadId, Data item);

    @EncodeMethod(id = 2)
    void remove(String name, String txnId, long threadId, Data item);

    @EncodeMethod(id = 3)
    void size(String name, String txnId, long threadId);

}
