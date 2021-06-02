/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

public enum PhoneHomeMetrics {
    //BUILD INFO METRICS
    HAZELCAST_DOWNLOAD_ID("p"),
    CLIENT_ENDPOINT_COUNT("cssz"),
    JAVA_VERSION_OF_SYSTEM("jvmv"),
    BUILD_VERSION("version"),
    JET_BUILD_VERSION("jetv"),

    //CLIENT INFO METRICS
    CLIENTS_WITH_CPP_CONNECTION("ccpp"),
    CLIENTS_WITH_CSHARP_CONNECTION("cdn"),
    CLIENTS_WITH_JAVA_CONNECTION("cjv"),
    CLIENTS_WITH_NODEJS_CONNECTION("cnjs"),
    CLIENTS_WITH_PYTHON_CONNECTION("cpy"),
    CLIENTS_WITH_GO_CONNECTION("cgo"),

    //CLUSTER INFO METRICS
    UUID_OF_CLUSTER("m"),
    CLUSTER_ID("c"),
    CLUSTER_SIZE("crsz"),
    TIME_TAKEN_TO_CLUSTER_UP("cuptm"),
    UPTIME_OF_RUNTIME_MXBEAN("nuptm"),
    RUNTIME_MXBEAN_VM_NAME("jvmn"),

    //OS INFO METRICS
    OPERATING_SYSTEM_NAME("osn"),
    OPERATING_SYSTEM_ARCH("osa"),
    OPERATING_SYSTEM_VERSION("osv"),

    // MAP METRICS
    COUNT_OF_MAPS("mpct"),
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
    CACHE_COUNT_WITH_WAN_REPLICATION("cawact"),

    //OTHER DISTRIBUTED OBJECTS
    COUNT_OF_SETS("sect"),
    COUNT_OF_QUEUES("quct"),
    COUNT_OF_MULTIMAPS("mmct"),
    COUNT_OF_LISTS("lict"),
    COUNT_OF_RING_BUFFERS("rbct"),
    COUNT_OF_TOPICS("tpct"),
    COUNT_OF_REPLICATED_MAPS("rpct"),
    COUNT_OF_CARDINALITY_ESTIMATORS("cect"),
    COUNT_OF_PN_COUNTERS("pncct"),
    COUNT_OF_FLAKE_ID_GENERATORS("figct"),

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
    DOCKER("dck");

    private final String query;

    PhoneHomeMetrics(String query) {
        this.query = query;
    }

    String getRequestParameterName() {
        return query;
    }
}
