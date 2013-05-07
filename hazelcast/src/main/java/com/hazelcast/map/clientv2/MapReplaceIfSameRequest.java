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

package com.hazelcast.map.clientv2;

import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.ReplaceIfSameOperation;
import com.hazelcast.map.ReplaceOperation;
import com.hazelcast.nio.serialization.Data;

public class MapReplaceIfSameRequest extends MapPutRequest {

    private Data oldValue;

    public MapReplaceIfSameRequest() {
    }

    public MapReplaceIfSameRequest(String name, Data key, Data value, Data oldValue, int threadId) {
        super(name, key, value, threadId);
        this.oldValue = oldValue;
    }

    public int getClassId() {
        return MapPortableHook.REPLACE_IF_SAME;
    }

    public Object process() throws Exception {
        ReplaceIfSameOperation op = new ReplaceIfSameOperation(name, key, oldValue, value);
        op.setThreadId(threadId);
        return clientEngine.invoke(getServiceName(), op, key);
    }

}
