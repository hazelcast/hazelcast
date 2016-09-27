/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduleexecutor.impl;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Thomas Kountis.
 */
public class BackupTaskDescriptor {

    private RunnableDefinition definition;

    private AmendableScheduledTaskStatistics masterStats;

    private Map masterState;

    public BackupTaskDescriptor() {
    }

    public BackupTaskDescriptor(RunnableDefinition definition) {
        this.definition = definition;
        this.masterStats = new ScheduledTaskStatisticsImpl();
        this.masterState = new HashMap();
    }

    public RunnableDefinition getDefinition() {
        return definition;
    }

    public AmendableScheduledTaskStatistics getMasterStats() {
        return masterStats;
    }

    public Map getMasterState() {
        return masterState;
    }

    public void setMasterState(Map masterState) {
        this.masterState = masterState;
    }
}
