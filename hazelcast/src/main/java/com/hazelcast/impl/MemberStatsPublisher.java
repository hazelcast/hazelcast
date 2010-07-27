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

import com.hazelcast.core.MessageListener;

public class MemberStatsPublisher implements MessageListener {
    final Node node;
    public final static String STATS_TOPIC_NAME = "_hz__MemberStatsTopic";
    public final static String STATS_MULTIMAP_NAME = "_hz__MemberStatsMultiMap";

    public MemberStatsPublisher(Node node) {
        this.node = node;
        node.factory.getTopic(STATS_TOPIC_NAME).addMessageListener(this);
    }

    public void onMessage(final Object key) {
        node.executorManager.executeLocally(new FallThroughRunnable() {
            public void doRun() {
                MemberStatsImpl memberStats = node.factory.createMemberStats();
                node.factory.getMultiMap(STATS_MULTIMAP_NAME).put(key, memberStats);
            }
        });
    }
}
