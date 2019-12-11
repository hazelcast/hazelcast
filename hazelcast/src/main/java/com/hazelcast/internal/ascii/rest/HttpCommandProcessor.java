/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.ascii.AbstractTextCommandProcessor;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.json.JsonValue;

import static com.hazelcast.internal.ascii.rest.HttpCommand.CONTENT_TYPE_BINARY;
import static com.hazelcast.internal.ascii.rest.HttpCommand.CONTENT_TYPE_JSON;
import static com.hazelcast.internal.ascii.rest.HttpCommand.CONTENT_TYPE_PLAIN_TEXT;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;


public abstract class HttpCommandProcessor<T> extends AbstractTextCommandProcessor<T> {
    public static final String URI_MAPS = "/hazelcast/rest/maps/";
    public static final String URI_QUEUES = "/hazelcast/rest/queues/";
    public static final String URI_WAN_BASE_URL = "/hazelcast/rest/wan";
    public static final String URI_HEALTH_URL = "/hazelcast/health";
    public static final String URI_HEALTH_READY = URI_HEALTH_URL + "/ready";

    // Instance
    public static final String URI_INSTANCE = "/hazelcast/rest/instance";

    // Cluster
    public static final String URI_CLUSTER = "/hazelcast/rest/cluster";
    public static final String URI_CLUSTER_MANAGEMENT_BASE_URL = "/hazelcast/rest/management/cluster";
    public static final String URI_CLUSTER_STATE_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/state";
    public static final String URI_CHANGE_CLUSTER_STATE_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/changeState";
    public static final String URI_CLUSTER_VERSION_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/version";
    public static final String URI_SHUTDOWN_CLUSTER_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/clusterShutdown";
    public static final String URI_SHUTDOWN_NODE_CLUSTER_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/memberShutdown";
    public static final String URI_CLUSTER_NODES_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/nodes";

    // Hot restart
    public static final String URI_FORCESTART_CLUSTER_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/forceStart";
    public static final String URI_PARTIALSTART_CLUSTER_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/partialStart";
    public static final String URI_HOT_RESTART_BACKUP_CLUSTER_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/hotBackup";
    public static final String URI_HOT_RESTART_BACKUP_INTERRUPT_CLUSTER_URL
            = URI_CLUSTER_MANAGEMENT_BASE_URL + "/hotBackupInterrupt";

    // WAN
    public static final String URI_WAN_SYNC_MAP = URI_WAN_BASE_URL + "/sync/map";
    public static final String URI_WAN_SYNC_ALL_MAPS = URI_WAN_BASE_URL + "/sync/allmaps";
    public static final String URI_WAN_CLEAR_QUEUES = URI_WAN_BASE_URL + "/clearWanQueues";
    public static final String URI_ADD_WAN_CONFIG = URI_WAN_BASE_URL + "/addWanConfig";
    public static final String URI_WAN_PAUSE_PUBLISHER = URI_WAN_BASE_URL + "/pausePublisher";
    public static final String URI_WAN_STOP_PUBLISHER = URI_WAN_BASE_URL + "/stopPublisher";
    public static final String URI_WAN_RESUME_PUBLISHER = URI_WAN_BASE_URL + "/resumePublisher";
    public static final String URI_WAN_CONSISTENCY_CHECK_MAP = URI_WAN_BASE_URL + "/consistencyCheck/map";

    // License info
    public static final String URI_LICENSE_INFO = "/hazelcast/rest/license";

    // CP Subsystem
    public static final String URI_CP_SUBSYSTEM_BASE_URL = "/hazelcast/rest/cp-subsystem";
    public static final String URI_RESET_CP_SUBSYSTEM_URL = URI_CP_SUBSYSTEM_BASE_URL + "/reset";
    public static final String URI_CP_GROUPS_URL = URI_CP_SUBSYSTEM_BASE_URL + "/groups";
    public static final String URI_CP_SESSIONS_SUFFIX = "/sessions";
    public static final String URI_REMOVE_SUFFIX = "/remove";
    public static final String URI_CP_MEMBERS_URL = URI_CP_SUBSYSTEM_BASE_URL + "/members";
    public static final String URI_LOCAL_CP_MEMBER_URL = URI_CP_MEMBERS_URL + "/local";

    protected HttpCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    protected void prepareResponse(HttpCommand command, Object value) {
        if (value == null) {
            command.send204();
        } else if (value instanceof byte[]) {
            command.setResponse(CONTENT_TYPE_BINARY, (byte[]) value);
        } else if (value instanceof RestValue) {
            RestValue restValue = (RestValue) value;
            command.setResponse(restValue.getContentType(), restValue.getValue());
        } else if (value instanceof HazelcastJsonValue || value instanceof JsonValue) {
            command.setResponse(CONTENT_TYPE_JSON, stringToBytes(value.toString()));
        } else if (value instanceof String) {
            command.setResponse(CONTENT_TYPE_PLAIN_TEXT, stringToBytes((String) value));
        } else {
            command.setResponse(CONTENT_TYPE_BINARY, textCommandService.toByteArray(value));
        }
    }
}
