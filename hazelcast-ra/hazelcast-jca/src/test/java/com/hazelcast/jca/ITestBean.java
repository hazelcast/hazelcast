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

package com.hazelcast.jca;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;

import javax.ejb.Local;
import javax.resource.ResourceException;
import javax.resource.cci.LocalTransaction;
import java.util.Collection;

@Local
public interface ITestBean {
    void insertToMap(String mapname, String key, String value);

    String getFromMap(String mapname, String key);

    void offerToQueue(String queuename, String key);

    String pollFromQueue(String queuename);

    void insertToSet(String setname, String key);

    boolean removeFromSet(String setname, String key);

    int getSetSize(String setname);

    void addToList(String listname, String key);

    boolean removeFromList(String listname, String key);

    int getListSize(String listname);

    void addDistributedObjectListener(DistributedObjectListener obj);

    void removeDistributedObjectListener(String regId);

    Collection<DistributedObject> getDistributedObjects();
}
