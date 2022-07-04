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

package com.hazelcast.auditlog;

/**
 * Auditable event type identifiers.
 *
 * @see AuditableEvent#typeId()
 */
public final class AuditlogTypeIds {

    // Network Events
    /**
     * Event type ID: Connection accepted.
     */
    public static final String NETWORK_CONNECT = "HZ-0101";
    /**
     * Event type ID: Connection closed.
     */
    public static final String NETWORK_DISCONNECT = "HZ-0102";
    /**
     * Event type ID: Protocol selected.
     */
    public static final String NETWORK_SELECT_PROTOCOL = "HZ-0103";

    // WAN
    /**
     * Event type ID: WAN sync.
     */
    public static final String WAN_SYNC = "HZ-0201";
    /**
     * Event type ID: WAN add config.
     */
    public static final String WAN_ADD_CONFIG = "HZ-0202";

    // Authentication
    /**
     * Event type ID: Client authentication.
     */
    public static final String AUTHENTICATION_CLIENT = "HZ-0501";
    /**
     * Event type ID: Member authentication.
     */
    public static final String AUTHENTICATION_MEMBER = "HZ-0502";
    /**
     * Event type ID: REST authentication.
     */
    public static final String AUTHENTICATION_REST = "HZ-0503";

    // Cluster events
    /**
     * Event type ID: Member joined.
     */
    public static final String CLUSTER_MEMBER_ADDED = "HZ-0601";
    /**
     * Event type ID: Member removed from the cluster.
     */
    public static final String CLUSTER_MEMBER_REMOVED = "HZ-0602";
    /**
     * Event type ID: Lite member promoted.
     */
    public static final String CLUSTER_PROMOTE_MEMBER = "HZ-0603";
    /**
     * Event type ID: Cluster shutdown.
     */
    public static final String CLUSTER_SHUTDOWN = "HZ-0604";
    /**
     * Event type ID: Cluster member suspected.
     */
    public static final String CLUSTER_MEMBER_SUSPECTED = "HZ-0605";
    /**
     * Event type ID: Clusters merged.
     */
    public static final String CLUSTER_MERGE = "HZ-0606";

    // Member events
    /**
     * Event type ID: Member logging level set.
     */
    public static final String MEMBER_LOGGING_LEVEL_SET = "HZ-0701";
    /**
     * Event type ID: Member logging level reset.
     */
    public static final String MEMBER_LOGGING_LEVEL_RESET = "HZ-0702";


    private AuditlogTypeIds() {
    }
}
