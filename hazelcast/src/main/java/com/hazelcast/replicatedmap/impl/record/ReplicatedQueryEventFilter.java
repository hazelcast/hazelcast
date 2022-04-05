/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.Map;

/**
 * This class is used to transfer a predicate as an remote operation and filter values matching the predicate
 */
public class ReplicatedQueryEventFilter extends ReplicatedEntryEventFilter {

    private Predicate predicate;

    public ReplicatedQueryEventFilter(Data key, Predicate predicate) {
        super(key);
        this.predicate = predicate;
    }

    public boolean eval(Object arg) {
        QueryableEntry entry = (QueryableEntry) arg;
        Data keyData = entry.getKeyData();
        return (key == null || key.equals(keyData)) && predicate.apply((Map.Entry) arg);
    }
}
