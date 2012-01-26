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

package com.hazelcast.monitor;

import com.hazelcast.core.Member;
import com.hazelcast.nio.DataSerializable;

import java.util.Set;

public interface ClusterStateView extends DataSerializable {

    Set<Member> getMembers();

    Set<String> getMaps();

    Set<String> getMultiMaps();

    Set<String> getQueues();

    Set<String> getSets();

    Set<String> getLists();

    Set<String> getTopics();

    Set<String> getIdGenerators();

    Set<String> getAtomicNumbers();

    Set<String> getCountDownLatches();

    Set<String> getSemaphores();

    int[] getPartitions(Member member);
}
