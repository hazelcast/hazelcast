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

package com.hazelcast.mapreduce.impl.notification;

import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.nio.Address;

import java.util.Map;

public class ReducerResultNotification extends MemberAwareMapReduceNotification {

    private Map reducedResults;
    private int partitionId;

    public ReducerResultNotification() {
    }

    public ReducerResultNotification(Address address, String name, String jobId,
                                     int partitionId, Map reducedResults) {
        super(address, name, jobId);
        this.reducedResults = reducedResults;
        this.partitionId = partitionId;
    }

    public Map getReducedResults() {
        return reducedResults;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.REDUCER_RESULT_MESSAGE;
    }

}
