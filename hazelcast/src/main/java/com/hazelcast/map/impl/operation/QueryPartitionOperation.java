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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.QueryResult;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryResultEntryImpl;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class QueryPartitionOperation extends AbstractMapOperation implements PartitionAwareOperation {

    private Predicate predicate;
    private QueryResult result;

    public QueryPartitionOperation(String mapName, Predicate predicate) {
        super(mapName);
        this.predicate = predicate;
    }

    public QueryPartitionOperation() {
    }

    public void run() {
        Collection<QueryableEntry> queryableEntries = mapService.getMapServiceContext().getMapContextQuerySupport()
                .queryOnPartition(name, predicate, getPartitionId());
        result = new QueryResult();
        for (QueryableEntry entry : queryableEntries) {
            result.add(new QueryResultEntryImpl(entry.getKeyData(), entry.getIndexKey(), entry.getValueData()));
        }
        final List<Integer> partitions = Collections.singletonList(getPartitionId());
        result.setPartitionIds(partitions);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(predicate);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        predicate = in.readObject();
    }
}
