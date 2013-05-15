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

package com.hazelcast.deprecated.collection.list.client;

import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.deprecated.nio.Protocol;

public class GetHandler extends ListCommandHandler {
    public GetHandler(CollectionService collectionService) {
        super(collectionService);
    }

    @Override
    protected Protocol processCall(ObjectListProxy proxy, Protocol protocol) {
        int index = Integer.valueOf(protocol.args[1]);
        Object o = proxy.get(index);
        return protocol.success(collectionService.getSerializationService().toData(o));
    }
}
