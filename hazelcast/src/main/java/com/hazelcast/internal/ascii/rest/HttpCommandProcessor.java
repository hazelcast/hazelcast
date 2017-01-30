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

package com.hazelcast.internal.ascii.rest;

import com.hazelcast.internal.ascii.AbstractTextCommandProcessor;
import com.hazelcast.internal.ascii.TextCommandService;

public abstract class HttpCommandProcessor<T> extends AbstractTextCommandProcessor<T> {
    public static final String URI_MAPS = "/hazelcast/rest/maps/";
    public static final String URI_QUEUES = "/hazelcast/rest/queues/";
    public static final String URI_CLUSTER = "/hazelcast/rest/cluster";
    public static final String URI_CLUSTER_MANAGEMENT_BASE_URL = "/hazelcast/rest/management/cluster";
    public static final String URI_MANCENTER_BASE_URL = "/hazelcast/rest/mancenter";
    public static final String URI_CLUSTER_STATE_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/state";
    public static final String URI_CHANGE_CLUSTER_STATE_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/changeState";
    public static final String URI_CLUSTER_VERSION_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/version";
    public static final String URI_SHUTDOWN_CLUSTER_URL =  URI_CLUSTER_MANAGEMENT_BASE_URL + "/clusterShutdown";
    public static final String URI_FORCESTART_CLUSTER_URL =  URI_CLUSTER_MANAGEMENT_BASE_URL + "/forceStart";
    public static final String URI_PARTIALSTART_CLUSTER_URL =  URI_CLUSTER_MANAGEMENT_BASE_URL + "/partialStart";
    public static final String URI_HOT_RESTART_BACKUP_CLUSTER_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/hotBackup";
    public static final String URI_HOT_RESTART_BACKUP_INTERRUPT_CLUSTER_URL
            = URI_CLUSTER_MANAGEMENT_BASE_URL + "/hotBackupInterrupt";
    public static final String URI_SHUTDOWN_NODE_CLUSTER_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/memberShutdown";
    public static final String URI_CLUSTER_NODES_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/nodes";
    public static final String URI_MANCENTER_CHANGE_URL = URI_MANCENTER_BASE_URL + "/changeurl";
    public static final String URI_WAN_SYNC_MAP = URI_MANCENTER_BASE_URL + "/wan/sync/map";
    public static final String URI_WAN_SYNC_ALL_MAPS = URI_MANCENTER_BASE_URL + "/wan/sync/allmaps";
    public static final String URI_MANCENTER_WAN_CLEAR_QUEUES = URI_MANCENTER_BASE_URL + "/wan/clearWanQueues";
    public static final String URI_ADD_WAN_CONFIG = URI_MANCENTER_BASE_URL + "/wan/addWanConfig";
    public static final String URI_HEALTH_URL = "/hazelcast/health";

    protected HttpCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }
}
