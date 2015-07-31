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

package com.hazelcast.client.impl.client;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.SerializableCollection;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Returns member list to client
 */
public class GetMemberListRequest extends CallableClientRequest implements RetryableRequest {

    public GetMemberListRequest() {
    }

    @Override
    public Object call() throws Exception {
        ClusterService service = getService();

        Collection<MemberImpl> memberList = service.getMemberList();
        Collection<Data> response = new ArrayList<Data>(memberList.size());
        for (MemberImpl member : memberList) {
            response.add(serializationService.toData(member));
        }
        return new SerializableCollection(response);
    }


    @Override
    public String getServiceName() {
        return ClusterServiceImpl.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return ClientPortableHook.GET_MEMBER_LIST;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
