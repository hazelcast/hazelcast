/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.phonehome;

/**
 * Enumeration of phone home metric types
 */
public enum PhoneHomeMetrics implements Metric {
    //BUILD INFO METRICS
    HAZELCAST_DOWNLOAD_ID("p"),
    CLIENT_ENDPOINT_COUNT("cssz"),
    JAVA_VERSION_OF_SYSTEM("jvmv"),
    BUILD_VERSION("version"),
    JAVA_CLASSPATH("classpath"),

    //CLIENT INFO METRICS
    ACTIVE_CPP_CLIENTS_COUNT("ccpp"),
    ACTIVE_CSHARP_CLIENTS_COUNT("cdn"),
    ACTIVE_JAVA_CLIENTS_COUNT("cjv"),
    ACTIVE_NODEJS_CLIENTS_COUNT("cnjs"),
    ACTIVE_PYTHON_CLIENTS_COUNT("cpy"),
    ACTIVE_GO_CLIENTS_COUNT("cgo"),
    ACTIVE_CL_CLIENTS_COUNT("ccl"),
    OPENED_CPP_CLIENT_CONNECTIONS_COUNT("ccppco"),
    OPENED_CSHARP_CLIENT_CONNECTIONS_COUNT("cdnco"),
    OPENED_JAVA_CLIENT_CONNECTIONS_COUNT("cjvco"),
    OPENED_NODEJS_CLIENT_CONNECTIONS_COUNT("cnjsco"),
    OPENED_PYTHON_CLIENT_CONNECTIONS_COUNT("cpyco"),
    OPENED_GO_CLIENT_CONNECTIONS_COUNT("cgoco"),
    OPENED_CL_CLIENT_CONNECTIONS_COUNT("cclco"),
    CLOSED_CPP_CLIENT_CONNECTIONS_COUNT("ccppcc"),
    CLOSED_CSHARP_CLIENT_CONNECTIONS_COUNT("cdncc"),
    CLOSED_JAVA_CLIENT_CONNECTIONS_COUNT("cjvcc"),
    CLOSED_NODEJS_CLIENT_CONNECTIONS_COUNT("cnjscc"),
    CLOSED_PYTHON_CLIENT_CONNECTIONS_COUNT("cpycc"),
    CLOSED_GO_CLIENT_CONNECTIONS_COUNT("cgocc"),
    CLOSED_CL_CLIENT_CONNECTIONS_COUNT("cclcc"),
    TOTAL_CPP_CLIENT_CONNECTION_DURATION("ccpptcd"),
    TOTAL_CSHARP_CLIENT_CONNECTION_DURATION("cdntcd"),
    TOTAL_JAVA_CLIENT_CONNECTION_DURATION("cjvtcd"),
    TOTAL_NODEJS_CLIENT_CONNECTION_DURATION("cnjstcd"),
    TOTAL_PYTHON_CLIENT_CONNECTION_DURATION("cpytcd"),
    TOTAL_GO_CLIENT_CONNECTION_DURATION("cgotcd"),
    TOTAL_CL_CLIENT_CONNECTION_DURATION("ccltcd"),
    CPP_CLIENT_VERSIONS("ccppcv"),
    CSHARP_CLIENT_VERSIONS("cdncv"),
    JAVA_CLIENT_VERSIONS("cjvcv"),
    NODEJS_CLIENT_VERSIONS("cnjscv"),
    PYTHON_CLIENT_VERSIONS("cpycv"),
    GO_CLIENT_VERSIONS("cgocv"),
    CL_CLIENT_VERSIONS("cclcv"),
    ALL_MEMBERS_CLIENTS_COUNT("allmembersclients"),
    SINGLE_MEMBER_CLIENTS_COUNT("singlememberclients"),
    MULTI_MEMBER_CLIENTS_COUNT("multimemberclients"),

    //CLUSTER INFO METRICS
    UUID_OF_CLUSTER("m"),
    CLUSTER_ID("c"),
    /**
     * Maintained for backward-compatibility (in PhoneHome reports), but otherwise deprecated, please use
     * {@link #EXACT_CLUSTER_SIZE} instead.
     */
    @Deprecated
    CLUSTER_SIZE("crsz"),
    EXACT_CLUSTER_SIZE("ecrsz"),
    TIME_TAKEN_TO_CLUSTER_UP("cuptm"),
    UPTIME_OF_RUNTIME_MXBEAN("nuptm"),
    RUNTIME_MXBEAN_VM_NAME("jvmn"),
    PARTITION_COUNT("parct"),

    //OS INFO METRICS
    OPERATING_SYSTEM_NAME("osn"),
    OPERATING_SYSTEM_ARCH("osa"),
    OPERATING_SYSTEM_VERSION("osv"),

    // MAP METRICS
    COUNT_OF_MAPS("mpct"),
    COUNT_OF_MAPS_ALL_TIME("mpcta"),
    MAP_COUNT_WITH_READ_ENABLED("mpbrct"),
    MAP_COUNT_WITH_MAP_STORE_ENABLED("mpmsct"),
    MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE("mpaoqcct"),
    MAP_COUNT_WITH_ATLEAST_ONE_INDEX("mpaoict"),
    MAP_COUNT_WITH_HOT_RESTART_OR_PERSISTENCE_ENABLED("mphect"),
    MAP_COUNT_WITH_WAN_REPLICATION("mpwact"),
    MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE("mpaocct"),
    MAP_COUNT_USING_EVICTION("mpevct"),
    MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT("mpnmct"),
    AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE("mpptlams"),
    AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE("mpptla"),
    AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE("mpgtlams"),
    AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE("mpgtla"),
    TOTAL_MAP_VALUES_CALLS("mpvaluesct"),
    TOTAL_MAP_ENTRYSET_CALLS("mpentriesct"),
    TOTAL_MAP_QUERY_SIZE_LIMITER_HITS("mpqslh"),

    //CACHE METRICS
    COUNT_OF_CACHES("cact"),
    COUNT_OF_CACHES_ALL_TIME("cacta"),
    CACHE_COUNT_WITH_WAN_REPLICATION("cawact"),

    //OTHER DISTRIBUTED OBJECTS
    COUNT_OF_SETS("sect"),
    COUNT_OF_SETS_ALL_TIME("secta"),
    COUNT_OF_QUEUES("quct"),
    COUNT_OF_QUEUES_ALL_TIME("qucta"),
    COUNT_OF_MULTIMAPS("mmct"),
    COUNT_OF_MULTIMAPS_ALL_TIME("mmcta"),
    COUNT_OF_LISTS("lict"),
    COUNT_OF_LISTS_ALL_TIME("licta"),
    COUNT_OF_RING_BUFFERS("rbct"),
    COUNT_OF_RING_BUFFERS_ALL_TIME("rbcta"),
    COUNT_OF_TOPICS("tpct"),
    COUNT_OF_TOPICS_ALL_TIME("tpcta"),
    COUNT_OF_REPLICATED_MAPS("rpct"),
    COUNT_OF_REPLICATED_MAPS_ALL_TIME("rpcta"),
    COUNT_OF_CARDINALITY_ESTIMATORS("cect"),
    COUNT_OF_CARDINALITY_ESTIMATORS_ALL_TIME("cecta"),
    COUNT_OF_PN_COUNTERS("pncct"),
    COUNT_OF_PN_COUNTERS_ALL_TIME("pnccta"),
    COUNT_OF_FLAKE_ID_GENERATORS("figct"),
    COUNT_OF_FLAKE_ID_GENERATORS_ALL_TIME("figcta"),

    //CLOUD METRICS
    /*
     *  it is a notation of the cloud env, if its G means it mean GCP , A means AWS , Z means Azure
     */
    CLOUD("cld"),
    /*
     *  it is a notation of the docker container env, if its
     *   K means the member runs in Kubernetes,
     *   D means it runs in docker but not on kubernetes ,
     *   N means it doesn't run in docker
     */

    DOCKER("dck"),

    /*
     *  Value of the environment variable "HZ_CLOUD_ENVIRONMENT" passed to this member.
     */
    VIRIDIAN("vrd"),

    //JET METRICS
    JET_ENABLED("jet"),
    JET_RESOURCE_UPLOAD_ENABLED("jetrsup"),
    JET_JOBS_SUBMITTED("jetjobss"),
    JET_CONNECTOR_COUNTS("jetcc"),

    // SQL METRICS
    SQL_QUERIES_SUBMITTED("sqlqs"),
    SQL_STREAMING_QUERIES_EXECUTED("sqlqse"),

    // DYNAMIC CONFIG PERSISTENCE
    DYNAMIC_CONFIG_PERSISTENCE_ENABLED("dcpe"),

    // STORAGE METRICS
    HD_MEMORY_ENABLED("hdme"),
    MEMORY_USED_HEAP_SIZE("muhs"),
    MEMORY_FREE_HEAP_SIZE("mfhs"),
    MEMORY_USED_NATIVE_SIZE("muns"),
    TIERED_STORAGE_ENABLED("tse"),
    DATA_MEMORY_COST("dmc"),

    // REST API metrics
    REST_ENABLED("restenabled"),
    REST_MAP_GET_SUCCESS("restmapgetsucc"),
    REST_MAP_GET_FAILURE("restmapgetfail"),
    REST_MAP_POST_SUCCESS("restmappostsucc"),
    REST_MAP_POST_FAILURE("restmappostfail"),
    REST_MAP_DELETE_SUCCESS("restmapdeletesucc"),
    REST_MAP_DELETE_FAILURE("restmapdeletefail"),
    REST_MAP_TOTAL_REQUEST_COUNT("restmaprequestct"),
    REST_ACCESSED_MAP_COUNT("restmapct"),

    REST_QUEUE_POST_SUCCESS("restqueuepostsucc"),
    REST_QUEUE_POST_FAILURE("restqueuepostfail"),
    REST_QUEUE_GET_SUCCESS("restqueuegetsucc"),
    REST_QUEUE_GET_FAILURE("restqueuegetfail"),
    REST_QUEUE_DELETE_SUCCESS("restqueuedeletesucc"),
    REST_QUEUE_DELETE_FAILURE("restqueuedeletefail"),
    REST_QUEUE_TOTAL_REQUEST_COUNT("restqueuerequestct"),
    REST_ACCESSED_QUEUE_COUNT("restqueuect"),

    REST_CONFIG_UPDATE_SUCCESS("restconfigupdatesucc"),
    REST_CONFIG_UPDATE_FAILURE("restconfigupdatefail"),
    REST_CONFIG_RELOAD_SUCCESS("restconfigreloadsucc"),
    REST_CONFIG_RELOAD_FAILURE("restconfigreloadfail"),

    REST_REQUEST_COUNT("restrequestct"),
    REST_UNIQUE_REQUEST_COUNT("restuniqrequestct"),

    UCN_ENABLED("ucnenabled"),
    UCN_NAMESPACE_COUNT("ucncount"),
    V_CPU_COUNT("vcpuct"),

    DIAGNOSTICS_DYNAMIC_ENABLED_COUNT("diagdenbct"),
    DIAGNOSTICS_DYNAMIC_AUTO_OFF_COUNT("diagdoffct");

    private final String query;

    PhoneHomeMetrics(String query) {
        this.query = query;
    }

    @Override
    public String getQueryParameter() {
        return query;
    }
}
