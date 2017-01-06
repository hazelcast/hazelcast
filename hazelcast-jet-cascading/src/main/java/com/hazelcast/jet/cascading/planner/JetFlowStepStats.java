/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cascading.planner;

import cascading.flow.FlowStep;
import cascading.management.state.ClientState;
import cascading.stats.FlowStepStats;

import java.util.ArrayList;
import java.util.Collection;

public class JetFlowStepStats extends FlowStepStats {

    public JetFlowStepStats(FlowStep flowStep, ClientState clientState) {
        super(flowStep, clientState);
    }

    @Override
    public void recordChildStats() {

    }

    @Override
    public String getProcessStepID() {
        return getID();
    }

    @Override
    public Collection<String> getCounterGroupsMatching(String regex) {
        return null;
    }

    @Override
    public void captureDetail(Type depth) {

    }

    @Override
    public long getLastSuccessfulCounterFetchTime() {
        return 0;
    }

    @Override
    public Collection<String> getCounterGroups() {
        return new ArrayList<>();
    }

    @Override
    public Collection<String> getCountersFor(String group) {
        return new ArrayList<>();
    }

    @Override
    public long getCounterValue(Enum counter) {
        return 0;
    }

    @Override
    public long getCounterValue(String group, String counter) {
        return 0;
    }
}
