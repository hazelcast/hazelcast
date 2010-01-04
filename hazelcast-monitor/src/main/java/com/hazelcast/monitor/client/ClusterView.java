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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ClusterView implements Serializable {

    private static final long serialVersionUID = -1447660859278549091L;


    private List<String> maps = new ArrayList<String>();
    private List<String> qs = new ArrayList<String>();
    private List<String> lists = new ArrayList<String>();
    private List<String> sets = new ArrayList<String>();
    private List<String> multiMaps = new ArrayList<String>();
    private List<String> topics = new ArrayList<String>();
    private List<String> locks = new ArrayList<String>();
    private List<String> members = new ArrayList<String>();

    private int id;
    private String groupName;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public List<String> getMaps() {
        return maps;
    }

    public List<String> getQs() {
        return qs;
    }

    public List<String> getLists() {
        return lists;
    }

    public List<String> getSets() {
        return sets;
    }

    public List<String> getMultiMaps() {
        return multiMaps;
    }

    public List<String> getTopics() {
        return topics;
    }

    public List<String> getLocks() {
        return locks;
    }

    public List<String> getMembers() {
        return members;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
}
