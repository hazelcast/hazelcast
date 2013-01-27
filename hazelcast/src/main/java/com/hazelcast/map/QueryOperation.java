/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class QueryOperation extends AbstractNamedOperation {

    MapService mapService;
    Predicate predicate;
    QueryResult result;

    public QueryOperation(String mapName, Predicate predicate) {
        super(mapName);
        this.predicate = predicate;
    }

    @Override
    public void run() throws Exception {
        mapService = getService();
        List<Integer> initialPartitions = mapService.getOwnedPartitions().get();
        IndexService indexService = mapService.getMapContainer(name).getIndexService();
        Set<QueryableEntry> entries = indexService.query(predicate, mapService.getQueryableEntrySet(name));
        List<Integer> finalPartitions = mapService.getOwnedPartitions().get();
//        System.out.println(finalPartitions.size() + " queryoperation " + entries.size());
        if (initialPartitions == finalPartitions) {
            result = new QueryResult();
            result.setPartitionIds(finalPartitions);
            result.setResult(entries);
        }
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeObject(predicate);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        predicate = in.readObject();
    }
}
