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

package com.hazelcast.jmx;

import com.hazelcast.config.Config;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @ali 2/1/13
 */
@ManagedDescription("HazelcastInstance")
public class InstanceMBean extends HazelcastMBean<HazelcastInstance> {

    Config config;
    Cluster cluster;

    protected InstanceMBean(HazelcastInstance managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = createObjectName(null, null);
        config = managedObject.getConfig();
        cluster = managedObject.getCluster();
    }

    @ManagedAnnotation("name")
    @ManagedDescription("Name of the Instance")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("config")
    @ManagedDescription("String representation of config")
    public String getConfig() {
        return config.toString();
    }

    @ManagedAnnotation("configSource")
    @ManagedDescription("The source of config")
    public String getConfigSource() {
        File configurationFile = config.getConfigurationFile();
        if (configurationFile != null) {
            return configurationFile.getAbsolutePath();
        }
        URL configurationUrl = config.getConfigurationUrl();
        if (configurationUrl != null) {
            return configurationUrl.toString();
        }
        return null;
    }

    @ManagedAnnotation("groupName")
    @ManagedDescription("Group Name")
    public String getGroupName(){
        return config.getGroupConfig().getName();
    }

    @ManagedAnnotation("port")
    @ManagedDescription("Network Port")
    public int getPort(){
        return config.getNetworkConfig().getPort();
    }

    @ManagedAnnotation("clusterTime")
    @ManagedDescription("Cluster-wide Time")
    public long getClusterTime(){
        return cluster.getClusterTime();
    }

    @ManagedAnnotation("memberCount")
    @ManagedDescription("size of the cluster")
    public int getMemberCount(){
        return cluster.getMembers().size();
    }

    @ManagedAnnotation("Members")
    @ManagedDescription("List of Members")
    public List<String> getMembers(){
        Set<Member> members = cluster.getMembers();
        List<String> list = new ArrayList<String>(members.size());
        for (Member member: members){
            list.add(member.getInetSocketAddress().toString());
        }
        return list;
    }

    @ManagedAnnotation("running")
    @ManagedDescription("Running state")
    public boolean isRunning(){
        return managedObject.getLifecycleService().isRunning();
    }

//    @ManagedAnnotation(value = "restart", operation = true)
//    @ManagedDescription("Restart the Node")
//    public void restart(){
//        managedObject.getLifecycleService().restart();
//    }

    @ManagedAnnotation(value = "shutdown", operation = true)
    @ManagedDescription("Shutdown the Node")
    public void shutdown(){
        managedObject.getLifecycleService().shutdown();
    }

}
