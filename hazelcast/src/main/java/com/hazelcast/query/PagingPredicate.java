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

package com.hazelcast.query;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.util.SortingUtil;

import java.io.IOException;
import java.util.*;

/**
 * @ali 07/12/13
 */
public class PagingPredicate implements IndexAwarePredicate, DataSerializable {

    private Predicate predicate;

    private Comparator comparator;

    private int pageSize;

    private int page;

    private Object anchor;

    public PagingPredicate() {
    }

    public PagingPredicate(int pageSize) {
        this.pageSize = pageSize;
    }

    public PagingPredicate(Predicate predicate, int pageSize) {
        this.predicate = predicate;
        this.pageSize = pageSize;
    }

    public PagingPredicate(Comparator comparator, int pageSize) {
        this.comparator = comparator;
        this.pageSize = pageSize;
    }

    public PagingPredicate(Predicate predicate, Comparator comparator, int pageSize) {
        this.predicate = predicate;
        this.comparator = comparator;
        this.pageSize = pageSize;
    }

    public Set<QueryableEntry> filter(QueryContext queryContext) {
        if (predicate instanceof IndexAwarePredicate) {
            Set<QueryableEntry> set = ((IndexAwarePredicate) predicate).filter(queryContext);
            if (set == null) {
                return null;
            }
            List<QueryableEntry> list = new LinkedList<QueryableEntry>();
            for (QueryableEntry entry : set) {
                if (anchor != null && SortingUtil.compare(comparator, anchor, entry.getValue()) >= 0) {
                    continue;
                }
                list.add(entry);
            }
            Collections.sort(list, new Comparator<QueryableEntry>() {
                public int compare(QueryableEntry o1, QueryableEntry o2) {
                    final Object value1 = o1.getValue();
                    final Object value2 = o2.getValue();
                    return SortingUtil.compare(comparator, value1, value2);
                }
            });
            if (list.size() > pageSize) {
                list = list.subList(0, pageSize);
            }
            return new LinkedHashSet<QueryableEntry>(list);
        }
        return null;
    }

    public boolean isIndexed(QueryContext queryContext) {
        if (predicate instanceof IndexAwarePredicate) {
            return ((IndexAwarePredicate) predicate).isIndexed(queryContext);
        }
        return false;
    }

    public boolean apply(Map.Entry mapEntry) {
        if (predicate != null) {
            return predicate.apply(mapEntry);
        }
        return true;
    }

    void setAnchor(Object anchor) {
        if (anchor != null) {
            this.anchor = anchor;
        }
    }

    public void nextPage() {
        page++;
    }

    public int getPage() {
        return page;
    }

    public int getPageSize() {
        return pageSize;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public Comparator getComparator() {
        return comparator;
    }

    public Object getAnchor() {
        return anchor;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(predicate);
        out.writeObject(comparator);
        out.writeInt(page);
        out.writeInt(pageSize);
        out.writeObject(anchor);
    }

    public void readData(ObjectDataInput in) throws IOException {
        predicate = in.readObject();
        comparator = in.readObject();
        page = in.readInt();
        pageSize = in.readInt();
        anchor = in.readObject();
    }

}
