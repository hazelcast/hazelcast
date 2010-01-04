/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.monitor.client;

import com.google.gwt.user.client.rpc.AsyncCallback;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;

import java.util.ArrayList;

public interface HazelcastServiceAsync {

    void connectCluster(String name, String pass, String ips,
                        AsyncCallback<ClusterView> callback) throws ConnectionExceptoin;

    void getChange(AsyncCallback<ArrayList<ChangeEvent>> async);

    void registerEvent(ChangeEventType eventType, int clusterId, String instanceName, AsyncCallback<ChangeEvent> async);

    void deRegisterEvent(ChangeEventType eventType, int clusterId, String instanceName, AsyncCallback<Void> async);

    void loadActiveClusterViews(AsyncCallback<ArrayList<ClusterView>> async);
}
