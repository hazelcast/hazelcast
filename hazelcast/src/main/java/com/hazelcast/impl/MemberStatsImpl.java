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

package com.hazelcast.impl;

import com.hazelcast.core.Member;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.monitor.MemberStats;

import java.util.HashMap;
import java.util.Map;

public class MemberStatsImpl implements MemberStats {
    MemberImpl member;
    Map<String, LocalMapStatsImpl> mapStats = new HashMap<String, LocalMapStatsImpl>();
    Map<String, LocalQueueStatsImpl> queueStats = new HashMap<String, LocalQueueStatsImpl>();

    public Member getMember() {
        return member;
    }

    public LocalMapStats getLocalMapStats(String mapName) {
        return mapStats.get(mapName);
    }

    public LocalQueueStats getLocalQueueStats(String queueName) {
        return queueStats.get(queueName);
    }
}