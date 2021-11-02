/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
public enum PhoneHomeMetrics {
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
    OPENED_CPP_CLIENT_CONNECTIONS_COUNT("ccppco"),
    OPENED_CSHARP_CLIENT_CONNECTIONS_COUNT("cdnco"),
    OPENED_JAVA_CLIENT_CONNECTIONS_COUNT("cjvco"),
    OPENED_NODEJS_CLIENT_CONNECTIONS_COUNT("cnjsco"),
    OPENED_PYTHON_CLIENT_CONNECTIONS_COUNT("cpyco"),
    OPENED_GO_CLIENT_CONNECTIONS_COUNT("cgoco"),
    CLOSED_CPP_CLIENT_CONNECTIONS_COUNT("ccppcc"),
    CLOSED_CSHARP_CLIENT_CONNECTIONS_COUNT("cdncc"),
    CLOSED_JAVA_CLIENT_CONNECTIONS_COUNT("cjvcc"),
    CLOSED_NODEJS_CLIENT_CONNECTIONS_COUNT("cnjscc"),
    CLOSED_PYTHON_CLIENT_CONNECTIONS_COUNT("cpycc"),
    CLOSED_GO_CLIENT_CONNECTIONS_COUNT("cgocc"),
    TOTAL_CPP_CLIENT_CONNECTION_DURATION("ccpptcd"),
    TOTAL_CSHARP_CLIENT_CONNECTION_DURATION("cdntcd"),
    TOTAL_JAVA_CLIENT_CONNECTION_DURATION("cjvtcd"),
    TOTAL_NODEJS_CLIENT_CONNECTION_DURATION("cnjstcd"),
    TOTAL_PYTHON_CLIENT_CONNECTION_DURATION("cpytcd"),
    TOTAL_GO_CLIENT_CONNECTION_DURATION("cgotcd"),
    CPP_CLIENT_VERSIONS("ccppcv"),
    CSHARP_CLIENT_VERSIONS("cdncv"),
    JAVA_CLIENT_VERSIONS("cjvcv"),
    NODEJS_CLIENT_VERSIONS("cnjscv"),
    PYTHON_CLIENT_VERSIONS("cpycv"),
    GO_CLIENT_VERSIONS("cgocv"),

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
    MAP_COUNT_WITH_HOT_RESTART_ENABLED("mphect"),
    MAP_COUNT_WITH_WAN_REPLICATION("mpwact"),
    MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE("mpaocct"),
    MAP_COUNT_USING_EVICTION("mpevct"),
    MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT("mpnmct"),
    AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE("mpptlams"),
    AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE("mpptla"),
    AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE("mpgtlams"),
    AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE("mpgtla"),

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

    //JET METRICS
    JET_ENABLED("jet"),
    JET_RESOURCE_UPLOAD_ENABLED("jetrsup"),
    JET_JOBS_SUBMITTED("jetjobss"),

    // SQL METRICS
    SQL_QUERIES_SUBMITTED("sqlqs"),

    //CP SUBSYSTEM METRICS
    CP_SUBSYSTEM_ENABLED("cp"),

    // REST API metrics
    REST_ENABLED("restenabled"),
    MAP_GET_COUNT("mapget200"),
    MAP_POST_SUCCESS("restmappostsucc"),
    MAP_POST_FAILURE("restmappostfail"),
    MAP_REQUEST_COUNT("restrequestct");

    private final String query;

    PhoneHomeMetrics(String query) {
        this.query = query;
    }

    String getRequestParameterName() {
        return query;
    }
}
