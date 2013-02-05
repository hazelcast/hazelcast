/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.collection.list.client;

import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.serialization.Data;

public class ListRemoveHandler extends ListCommandHandler {
    public ListRemoveHandler(CollectionService collectionService) {
        super(collectionService);
    }

    @Override
    protected Protocol processCall(ObjectListProxy proxy, Protocol protocol) {
        if (protocol.hasBuffer()) {
            Data item = protocol.buffers[0];
            boolean added = proxy.remove(item);
            return protocol.success(String.valueOf(added));    
        } else {
            int index = Integer.valueOf(protocol.args[1]);
            Object o = proxy.remove(index);
            return protocol.success(collectionService.getSerializationService().toData(o));
        }
    }
}