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

package com.hazelcast.map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.QueryResultEntry;
import com.hazelcast.query.impl.QueryResultEntryImpl;

import java.io.IOException;
import java.util.*;

public class QueryResult implements DataSerializable {

    private List<Integer> partitionIds;
    private final Set<QueryResultEntry> result = new LinkedHashSet<QueryResultEntry>();

    public List<Integer> getPartitionIds() {
        return partitionIds;
    }

    public void setPartitionIds(List<Integer> partitionIds) {
        this.partitionIds = partitionIds;
    }

    public void add(QueryResultEntry resultEntry) {
        result.add(resultEntry);
    }

    public Set<QueryResultEntry> getResult() {
        return result;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        int psize = (partitionIds == null) ? 0 : partitionIds.size();
        out.writeInt(psize);
        for (int i = 0; i < psize; i++) {
            out.writeInt(partitionIds.get(i));
        }
        int rsize = result.size();
        out.writeInt(rsize);
        if (rsize > 0) {
            Iterator<QueryResultEntry> iterator = result.iterator();
            for (int i = 0; i < rsize; i++) {
                final QueryResultEntryImpl queryableEntry = (QueryResultEntryImpl) iterator.next();
                queryableEntry.writeData(out);
            }
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        int psize = in.readInt();
        if (psize > 0) {
            partitionIds = new ArrayList<Integer>(psize);
            for (int i = 0; i < psize; i++) {
                partitionIds.add(in.readInt());
            }
        }
        int rsize = in.readInt();
        if (rsize > 0) {
            for (int i = 0; i < rsize; i++) {
                final QueryResultEntryImpl resultEntry = new QueryResultEntryImpl();
                resultEntry.readData(in);
                result.add(resultEntry);
            }
        }
    }
}
