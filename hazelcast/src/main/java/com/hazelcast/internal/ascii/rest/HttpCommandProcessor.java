/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.auditlog.AuditlogTypeIds;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.ascii.AbstractTextCommandProcessor;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.UsernamePasswordCredentials;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLDecoder;

import static com.hazelcast.internal.ascii.rest.HttpCommand.CONTENT_TYPE_BINARY;
import static com.hazelcast.internal.ascii.rest.HttpCommand.CONTENT_TYPE_JSON;
import static com.hazelcast.internal.ascii.rest.HttpCommand.CONTENT_TYPE_PLAIN_TEXT;
import static com.hazelcast.internal.ascii.rest.HttpCommandProcessor.ResponseType.FAIL;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_200;
import static com.hazelcast.internal.util.StringUtil.bytesToString;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;


public abstract class HttpCommandProcessor<T extends HttpCommand> extends AbstractTextCommandProcessor<T> {
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

    // Persistence
    public static final String URI_FORCESTART_CLUSTER_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/forceStart";
    public static final String URI_PARTIALSTART_CLUSTER_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/partialStart";
    public static final String URI_PERSISTENCE_BACKUP_CLUSTER_URL = URI_CLUSTER_MANAGEMENT_BASE_URL + "/backup";
    public static final String URI_PERSISTENCE_BACKUP_INTERRUPT_CLUSTER_URL
            = URI_CLUSTER_MANAGEMENT_BASE_URL + "/backupInterrupt";
    // Deprecated endpoints
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

    // Log Level
    public static final String URI_LOG_LEVEL = "/hazelcast/rest/log-level";
    public static final String URI_LOG_LEVEL_RESET = "/hazelcast/rest/log-level/reset";

    // Config
    public static final String URI_CONFIG = "/hazelcast/rest/config";
    public static final String URI_CONFIG_RELOAD = URI_CONFIG + "/reload";
    public static final String URI_CONFIG_UPDATE = URI_CONFIG + "/update";


    protected final ILogger logger;
    protected final RestCallCollector restCallCollector;

    protected HttpCommandProcessor(TextCommandService textCommandService, ILogger logger) {
        super(textCommandService);
        this.logger = logger;
        this.restCallCollector = textCommandService.getRestCallCollector();
    }

    /**
     * If the {@code value} is {@code null}, prepares a
     * {@link HttpURLConnection#HTTP_NO_CONTENT} response, otherwise prepares an
     * {@link HttpURLConnection#HTTP_OK} response with the provided response value.
     *
     * @param command the HTTP request
     * @param value   the response value to send
     */
    protected void prepareResponse(@Nonnull HttpCommand command,
                                   @Nullable Object value) {
        if (value == null) {
            command.send204();
        } else {
            prepareResponse(SC_200, command, value);
        }
    }

    /**
     * Prepares a response with the provided status line and response value.
     *
     * @param statusCode the HTTP response status code
     * @param command    the HTTP request
     * @param value      the response value to send
     */
    protected void prepareResponse(HttpStatusCode statusCode,
                                   @Nonnull HttpCommand command,
                                   @Nonnull Object value) {
        if (value instanceof byte[]) {
            command.setResponse(statusCode, CONTENT_TYPE_BINARY, (byte[]) value);
        } else if (value instanceof RestValue) {
            RestValue restValue = (RestValue) value;
            command.setResponse(statusCode, restValue.getContentType(), restValue.getValue());
        } else if (value instanceof HazelcastJsonValue || value instanceof JsonValue) {
            command.setResponse(statusCode, CONTENT_TYPE_JSON, stringToBytes(value.toString()));
        } else if (value instanceof String) {
            command.setResponse(statusCode, CONTENT_TYPE_PLAIN_TEXT, stringToBytes((String) value));
        } else {
            command.setResponse(statusCode, CONTENT_TYPE_BINARY, textCommandService.toByteArray(value));
        }
    }

    /**
     * Decodes HTTP post params contained in {@link HttpPostCommand#getData()}. The data
     * should be encoded in UTF-8 and joined together with an ampersand (&).
     *
     * @param command            the HTTP post command
     * @param expectedParamCount the number of parameters expected in the command
     * @return the decoded params
     * @throws UnsupportedEncodingException If character encoding needs to be consulted, but
     *                                      named character encoding is not supported
     * @throws HttpBadRequestException      in case the command did not contain at least {@code expectedParamCount}
     *                                      parameters
     */
    private static @Nonnull
    String[] decodeParams(HttpPostCommand command, int expectedParamCount)
            throws UnsupportedEncodingException {
        byte[] data = command.getData();
        if (data == null) {
            throw new HttpBadRequestException(
                    "This endpoint expects at least " + expectedParamCount + " parameters");
        }

        String[] encoded = bytesToString(data).split("&", expectedParamCount);
        String[] decoded = new String[encoded.length];

        if (encoded.length < expectedParamCount) {
            throw new HttpBadRequestException(
                    "This endpoint expects at least " + expectedParamCount + " parameters");
        }

        for (int i = 0; i < expectedParamCount; i++) {
            decoded[i] = URLDecoder.decode(encoded[i], "UTF-8");
        }
        return decoded;
    }

    protected String[] decodeParamsAndAuthenticate(HttpPostCommand cmd, int expectedParamCount)
            throws UnsupportedEncodingException {
        String[] params = decodeParams(cmd, expectedParamCount);
        if (!authenticate(cmd, params[0], params[1])) {
            throw new HttpForbiddenException();
        }
        return params;
    }

    /**
     * Checks if the request is valid. If Hazelcast Security is not enabled,
     * then only the given user name is compared to cluster name in node
     * configuration. Otherwise member JAAS authentication (member login module
     * stack) is used to authenticate the command.
     *
     * @param command  the HTTP request
     * @param userName URL-encoded username
     * @param pass     URL-encoded password
     * @return if the request has been successfully authenticated
     * @throws UnsupportedEncodingException If character encoding needs to be consulted, but named character encoding
     *                                      is not supported
     */
    private boolean authenticate(@Nonnull HttpPostCommand command,
                                 @Nullable String userName,
                                 @Nullable String pass)
            throws UnsupportedEncodingException {
        String decodedName = userName != null ? URLDecoder.decode(userName, "UTF-8") : null;
        SecurityContext securityContext = getNode().getNodeExtension().getSecurityContext();
        String clusterName = getNode().getConfig().getClusterName();
        if (securityContext == null) {
            if (pass != null && !pass.isEmpty()) {
                logger.fine("Password was provided but the Hazelcast Security is disabled.");
            }
            return clusterName.equals(decodedName);
        }
        String decodedPass = pass != null ? URLDecoder.decode(pass, "UTF-8") : null;
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(decodedName, decodedPass);
        Boolean passed = Boolean.FALSE;
        try {
            // we don't have an argument for clusterName in HTTP request, so let's reuse the "username" here
            LoginContext lc = securityContext.createMemberLoginContext(decodedName, credentials, command.getConnection());
            lc.login();
            passed = Boolean.TRUE;
        } catch (LoginException e) {
            return false;
        } finally {
            textCommandService.getNode().getNodeExtension().getAuditlogService()
                .eventBuilder(AuditlogTypeIds.AUTHENTICATION_REST)
                .message("REST connection authentication.")
                .addParameter("user", userName)
                .addParameter("command", command)
                .addParameter("passed", passed)
                .log();
        }
        return true;
    }

    protected void sendResponse(HttpPostCommand command, JsonObject json) {
        prepareResponse(command, json);
        textCommandService.sendResponse(command);
    }

    protected static JsonObject exceptionResponse(Throwable throwable) {
        return response(FAIL, "message", throwable.getMessage());
    }

    protected static JsonObject response(ResponseType type, String... attributes) {
        JsonObject object = new JsonObject()
                .add("status", type.toString());
        if (attributes.length > 0) {
            for (int i = 0; i < attributes.length; ) {
                String key = attributes[i++];
                String value = attributes[i++];
                if (value != null) {
                    object.add(key, value);
                }
            }
        }
        return object;
    }

    protected Node getNode() {
        return textCommandService.getNode();
    }

    protected enum ResponseType {
        SUCCESS, FAIL;

        @Override
        public String toString() {
            return super.toString().toLowerCase(StringUtil.LOCALE_INTERNAL);
        }
    }
}
