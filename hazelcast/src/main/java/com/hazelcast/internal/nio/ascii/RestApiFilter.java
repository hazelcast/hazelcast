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

package com.hazelcast.internal.nio.ascii;

import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestEndpointGroup;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;

import java.util.StringTokenizer;

import static com.hazelcast.jet.impl.util.Util.CONFIG_CHANGE_TEMPLATE;
import static java.lang.String.format;

/**
 * This class is a policy enforcement point for HTTP REST API. It checks incoming command lines and validates if the command can
 * be processed. If the command is unknown or not allowed the connection is closed.
 */
public class RestApiFilter implements TextProtocolFilter {

    private final ILogger logger;
    private final RestApiConfig restApiConfig;
    private final TextParsers parsers;

    RestApiFilter(LoggingService loggingService, RestApiConfig restApiConfig, TextParsers parsers) {
        this.logger = loggingService.getLogger(getClass());
        this.restApiConfig = restApiConfig;
        this.parsers = parsers;
    }

    @Override
    public void filterConnection(String commandLine, ServerConnection connection) {
        RestEndpointGroup restEndpointGroup = getEndpointGroup(commandLine);
        if (restEndpointGroup != null) {
            if (!restApiConfig.isGroupEnabled(restEndpointGroup)) {
                String name = restEndpointGroup.name();
                connection.close("REST endpoint group is not enabled - " + restEndpointGroup
                        + ". To enable it, do one of the following:\n"
                        + format(CONFIG_CHANGE_TEMPLATE,
                        "config.getNetworkConfig().getRestApiConfig().enableGroups(RestEndpointGroup." + name + ");",
                        "hazelcast.network.rest-api.endpoint-group " + name + " with `enabled` set to true",
                        "-Dhz.network.rest-api.endpoint-groups." + name.toLowerCase() + ".enabled=true",
                        "HZ_NETWORK_RESTAPI_ENDPOINTGROUPS." + name + ".ENABLED=true"),
                        null);
            }
        } else if (!commandLine.isEmpty()) {
            connection.close("Unsupported command received on REST API handler.", null);
        }
    }

    /**
     * Parses given command line and return corresponding {@link RestEndpointGroup} instance, or {@code null} if no such is
     * found.
     */
    private RestEndpointGroup getEndpointGroup(String commandLine) {
        if (commandLine == null) {
            return null;
        }
        StringTokenizer st = new StringTokenizer(commandLine);
        String operation = nextToken(st);
        // If the operation doesn't have a parser, then it's unknown.
        if (parsers.getParser(operation) == null) {
            return null;
        }
        // the operation is an HTTP method so the next token should be a resource path
        String requestUri = nextToken(st);
        return requestUri != null ? getHttpApiEndpointGroup(operation, requestUri) : null;
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity",
            "checkstyle:booleanexpressioncomplexity"})
    private RestEndpointGroup getHttpApiEndpointGroup(String operation, String requestUri) {
        if (requestUri.startsWith(HttpCommandProcessor.URI_MAPS)
                || requestUri.startsWith(HttpCommandProcessor.URI_QUEUES)) {
            return RestEndpointGroup.DATA;
        }
        if (requestUri.startsWith(HttpCommandProcessor.URI_HEALTH_URL)) {
            return RestEndpointGroup.HEALTH_CHECK;
        }
        if (requestUri.startsWith(HttpCommandProcessor.URI_WAN_BASE_URL)) {
            return RestEndpointGroup.WAN;
        }
        if (requestUri.startsWith(HttpCommandProcessor.URI_FORCESTART_CLUSTER_URL)
                || requestUri.startsWith(HttpCommandProcessor.URI_PARTIALSTART_CLUSTER_URL)
                || requestUri.startsWith(HttpCommandProcessor.URI_PERSISTENCE_BACKUP_CLUSTER_URL)
                || requestUri.startsWith(HttpCommandProcessor.URI_PERSISTENCE_BACKUP_INTERRUPT_CLUSTER_URL)
                // deprecated
                || requestUri.startsWith(HttpCommandProcessor.URI_HOT_RESTART_BACKUP_CLUSTER_URL)
                // deprecated
                || requestUri.startsWith(HttpCommandProcessor.URI_HOT_RESTART_BACKUP_INTERRUPT_CLUSTER_URL)
        ) {
            return RestEndpointGroup.PERSISTENCE;
        }
        if (requestUri.startsWith(HttpCommandProcessor.URI_CLUSTER)
                || requestUri.startsWith(HttpCommandProcessor.URI_CLUSTER_STATE_URL)
                || requestUri.startsWith(HttpCommandProcessor.URI_CLUSTER_NODES_URL)
                || ("GET".equals(operation) && requestUri.startsWith(HttpCommandProcessor.URI_LICENSE_INFO))
                || ("GET".equals(operation) && requestUri.startsWith(HttpCommandProcessor.URI_CLUSTER_VERSION_URL))
                || requestUri.startsWith(HttpCommandProcessor.URI_INSTANCE)
                || ("GET".equals(operation) && requestUri.startsWith(HttpCommandProcessor.URI_LOG_LEVEL))) {
            return RestEndpointGroup.CLUSTER_READ;
        }
        if (requestUri.startsWith(HttpCommandProcessor.URI_SHUTDOWN_CLUSTER_URL)
                || requestUri.startsWith(HttpCommandProcessor.URI_SHUTDOWN_NODE_CLUSTER_URL)
                || requestUri.startsWith(HttpCommandProcessor.URI_CHANGE_CLUSTER_STATE_URL)
                || requestUri.startsWith(HttpCommandProcessor.URI_CLUSTER_VERSION_URL)
                || requestUri.startsWith(HttpCommandProcessor.URI_LICENSE_INFO)
                || requestUri.startsWith(HttpCommandProcessor.URI_LOG_LEVEL)
                || requestUri.startsWith(HttpCommandProcessor.URI_CONFIG_RELOAD)
                || requestUri.startsWith(HttpCommandProcessor.URI_CONFIG_UPDATE)) {
            return RestEndpointGroup.CLUSTER_WRITE;
        }
        if (requestUri.startsWith(HttpCommandProcessor.URI_CP_SUBSYSTEM_BASE_URL)) {
            return RestEndpointGroup.CP;
        }

        logger.warning("No REST group matching URI: " + requestUri);
        return null;
    }

    private String nextToken(StringTokenizer st) {
        return st.hasMoreTokens() ? st.nextToken() : null;
    }
}
