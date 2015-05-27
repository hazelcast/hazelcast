/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.template;

import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Nullable;
import com.hazelcast.annotation.Response;
import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.mapreduce.JobPartitionState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Client Protocol Responses
 */
@GenerateCodec(id = 0, name = "response", ns = "")
public interface ResponseTemplate {

    @Response(ResponseMessageConst.VOID)
    void Void();

    @Response(ResponseMessageConst.BOOLEAN)
    void Boolean(boolean response);

    @Response(ResponseMessageConst.INTEGER)
    void Integer(int response);

    @Response(ResponseMessageConst.LONG)
    void Long(long response);

    @Response(ResponseMessageConst.STRING)
    void String(String response);

    @Response(ResponseMessageConst.DATA)
    void Data(@Nullable Data response);

    @Response(ResponseMessageConst.LIST_DATA)
    void ListData(List<Data> list);

    @Response(ResponseMessageConst.MAP_DATA_DATA)
    void MapDataData(Map<Data, Data> map);

    @Response(ResponseMessageConst.MAP_INT_DATA)
    void MapIntData(Map<Integer, Data> map);

    @Response(ResponseMessageConst.AUTHENTICATION)
    void Authentication(Address address, String uuid, String ownerUuid);

    @Response(ResponseMessageConst.PARTITIONS)
    void Partitions(Address[] members, int[] ownerIndexes);

    @Response(ResponseMessageConst.EXCEPTION)
    void Exception();

    @Response(ResponseMessageConst.DISTRIBUTED_OBJECT)
    void DistributedObject(Collection<DistributedObjectInfo> infoCollection);

    @Response(ResponseMessageConst.ENTRY_VIEW)
    void EntryView(@Nullable SimpleEntryView<Data, Data> dataEntryView);

    @Response(ResponseMessageConst.JOB_PROCESS_INFO)
    void JobProcessInfo(JobPartitionState[] jobPartitionStates, int processRecords);

    @Response(ResponseMessageConst.SET_DATA)
    void SetData(Set<Data> list);

    @Response(ResponseMessageConst.SET_ENTRY)
    void SetEntry(List<Data> keys, List<Data> values);

}
