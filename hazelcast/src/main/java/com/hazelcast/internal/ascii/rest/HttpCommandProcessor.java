/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.ascii.AbstractTextCommandProcessor;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.util.rest.RestUtil;
import com.hazelcast.logging.ILogger;

import static com.hazelcast.internal.util.rest.RestUtil.exceptionResponse;
import static com.hazelcast.internal.util.rest.RestUtil.response;
import static com.hazelcast.internal.util.rest.RestUtil.sendResponse;
import static com.hazelcast.util.StringUtil.lowerCaseInternal;
import static com.hazelcast.util.StringUtil.stringToBytes;


public abstract class HttpCommandProcessor<T> extends AbstractTextCommandProcessor<T> {
    public static final String URI_MAPS = "/hazelcast/rest/maps/";
    public static final String URI_QUEUES = "/hazelcast/rest/queues/";
    public static final String URI_MANCENTER_BASE_URL = "/hazelcast/rest/mancenter";
    public static final String URI_MANCENTER_CHANGE_URL = URI_MANCENTER_BASE_URL + "/changeurl";
    public static final String URI_UPDATE_PERMISSIONS = URI_MANCENTER_BASE_URL + "/security/permissions";
    public static final String URI_HEALTH_URL = "/hazelcast/health";

    // Cluster
    public static final String URI_CLUSTER = "/hazelcast/rest/cluster";
    public static final String URI_CLUSTER_MANAGEMENT_BASE_URL = "/hazelcast/rest/management/cluster";
    public static final String URI_CLUSTER_STATE_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/state";
    public static final String URI_CHANGE_CLUSTER_STATE_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/changeState";
    public static final String URI_CLUSTER_VERSION_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/version";
    public static final String URI_SHUTDOWN_CLUSTER_URL =  URI_CLUSTER_MANAGEMENT_BASE_URL + "/clusterShutdown";
    public static final String URI_SHUTDOWN_NODE_CLUSTER_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/memberShutdown";
    public static final String URI_CLUSTER_NODES_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/nodes";

    // Hot restart
    public static final String URI_FORCESTART_CLUSTER_URL =  URI_CLUSTER_MANAGEMENT_BASE_URL + "/forceStart";
    public static final String URI_PARTIALSTART_CLUSTER_URL =  URI_CLUSTER_MANAGEMENT_BASE_URL + "/partialStart";
    public static final String URI_HOT_RESTART_BACKUP_CLUSTER_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/hotBackup";
    public static final String URI_HOT_RESTART_BACKUP_INTERRUPT_CLUSTER_URL
            = URI_CLUSTER_MANAGEMENT_BASE_URL + "/hotBackupInterrupt";

    // WAN
    public static final String URI_WAN_SYNC_MAP = URI_MANCENTER_BASE_URL + "/wan/sync/map";
    public static final String URI_WAN_SYNC_ALL_MAPS = URI_MANCENTER_BASE_URL + "/wan/sync/allmaps";
    public static final String URI_MANCENTER_WAN_CLEAR_QUEUES = URI_MANCENTER_BASE_URL + "/wan/clearWanQueues";
    public static final String URI_ADD_WAN_CONFIG = URI_MANCENTER_BASE_URL + "/wan/addWanConfig";
    public static final String URI_WAN_PAUSE_PUBLISHER = URI_MANCENTER_BASE_URL + "/wan/pausePublisher";
    public static final String URI_WAN_STOP_PUBLISHER = URI_MANCENTER_BASE_URL + "/wan/stopPublisher";
    public static final String URI_WAN_RESUME_PUBLISHER = URI_MANCENTER_BASE_URL + "/wan/resumePublisher";
    public static final String URI_WAN_CONSISTENCY_CHECK_MAP = URI_MANCENTER_BASE_URL + "/wan/consistencyCheck/map";

    public static final String LEGACY_URI_WAN_SYNC_MAP = "/hazelcast/rest/wan/sync/map";
    public static final String LEGACY_URI_WAN_SYNC_ALL_MAPS = "/hazelcast/rest/wan/sync/allmaps";
    public static final String LEGACY_URI_MANCENTER_WAN_CLEAR_QUEUES = "/hazelcast/rest/mancenter/clearWanQueues";
    public static final String LEGACY_URI_ADD_WAN_CONFIG = "/hazelcast/rest/wan/addWanConfig";

    private final ILogger logger;

    protected HttpCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
        this.logger = textCommandService.getNode().getLogger(HttpCommandProcessor.class);
    }

    public void handleGetClusterState(HttpCommand command) {
        String res;
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            ClusterState clusterState = clusterService.getClusterState();
            res = response(RestUtil.ResponseType.SUCCESS, "state", lowerCaseInternal(clusterState.toString()));
        } catch (Throwable throwable) {
            logger.warning("Error occurred while getting cluster state", throwable);
            res = exceptionResponse(throwable);
        }
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
    }

    public void handleListNodes(HttpCommand command) {
        String res;
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            final String responseTxt = clusterService.getMembers().toString() + "\n"
                    + node.getBuildInfo().getVersion() + "\n"
                    + System.getProperty("java.version");
            res = response(RestUtil.ResponseType.SUCCESS, "response", responseTxt);
            sendResponse(textCommandService, command, res);
            return;
        } catch (Throwable throwable) {
            logger.warning("Error occurred while listing nodes", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(textCommandService, command, res);
    }

    public void handleClusterShutdown(HttpCommand command) {
        String res;
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            res = response(RestUtil.ResponseType.SUCCESS);
            sendResponse(textCommandService, command, res);
            clusterService.shutdown();
            return;
        } catch (Throwable throwable) {
            logger.warning("Error occurred while shutting down cluster", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(textCommandService, command, res);
    }

    public void handleShutdownNode(HttpCommand command) {
        String res;
        try {
            Node node = textCommandService.getNode();
            res = response(RestUtil.ResponseType.SUCCESS);
            sendResponse(textCommandService, command, res);
            node.hazelcastInstance.shutdown();
            return;
        } catch (Throwable throwable) {
            logger.warning("Error occurred while shutting down", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(textCommandService, command, res);
    }

    public void handleForceStart(HttpCommand command) {
        String res;
        try {
            Node node = textCommandService.getNode();
            boolean success = node.getNodeExtension().getInternalHotRestartService().triggerForceStart();
            res = response(success ? RestUtil.ResponseType.SUCCESS : RestUtil.ResponseType.FAIL);
        } catch (Throwable throwable) {
            logger.warning("Error occurred while handling force start", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(textCommandService, command, res);
    }

    public void handlePartialStart(HttpCommand command) {
        String res;
        try {
            Node node = textCommandService.getNode();
            boolean success = node.getNodeExtension().getInternalHotRestartService().triggerPartialStart();
            res = response(success ? RestUtil.ResponseType.SUCCESS : RestUtil.ResponseType.FAIL);
        } catch (Throwable throwable) {
            logger.warning("Error occurred while handling partial start", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(textCommandService, command, res);
    }

    public void handleHotRestartBackup(HttpCommand command) {
        String res;
        try {
            textCommandService.getNode().getNodeExtension().getHotRestartService().backup();
            res = response(RestUtil.ResponseType.SUCCESS);
        } catch (Throwable throwable) {
            logger.warning("Error occurred while invoking hot backup", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(textCommandService, command, res);
    }

    public void handleHotRestartBackupInterrupt(HttpCommand command) {
        String res;
        try {
            textCommandService.getNode().getNodeExtension().getHotRestartService().interruptBackupTask();
            res = response(RestUtil.ResponseType.SUCCESS);
        } catch (Throwable throwable) {
            logger.warning("Error occurred while interrupting hot backup", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(textCommandService, command, res);
    }
}
