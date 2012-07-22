/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.map;

import com.hazelcast.impl.spi.NodeService;
import com.hazelcast.nio.Data;

import java.util.Map;

import static com.hazelcast.impl.map.MapService.MAP_SERVICE_NAME;
import static com.hazelcast.nio.IOUtil.toData;

public class MapProxy {
    final NodeService nodeService;

    public MapProxy(NodeService nodeService) {
        this.nodeService = nodeService;
    }

    public Object put(String name, Object k, Object v, long ttl) {
        Data key = toData(k);
        int partitionId = nodeService.getPartitionId(key);
        PutOperation putOperation = new PutOperation(name, toData(k), v, ttl);
        try {
            return nodeService.invokeOptimistically(MAP_SERVICE_NAME, putOperation, partitionId).get();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public Object getOperation(String name, Object k) {
        Data key = toData(k);
        int partitionId = nodeService.getPartitionId(key);
        GetOperation getOperation = new GetOperation(name, toData(k));
        try {
            Object response = nodeService.invokeOptimistically(MAP_SERVICE_NAME, getOperation, partitionId).get();
            System.out.println("response is " + response);
            return response;
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public int getSize(String name) {
        try {
            Map<Integer, Object> results = nodeService.invokeOnAllPartitions(MAP_SERVICE_NAME, new MapSizeOperation(name));
            int total = 0;
            for (Object result : results.values()) {
                Integer size = (Integer) result;
                System.out.println(">> " + size);
                total += size;
            }
            return total;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}
