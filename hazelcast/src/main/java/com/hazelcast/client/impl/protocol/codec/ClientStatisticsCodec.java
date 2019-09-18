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

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

/**
 * The statistics is a String that is composed of key=value pairs separated by ',' . The following characters
 * ('=' '.' ',' '\') should be escaped in IMap and ICache names by the escape character ('\'). E.g. if the map name is
 * MyMap.First, it will be escaped as: MyMap\.First
 * 
 * The statistics key identify the category and name of the statistics. It is formatted as:
 * mainCategory.subCategory.statisticName
 * 
 * An e.g. Operating system committedVirtualMemorySize path would be: os.committedVirtualMemorySize
 * 
 * Please note that if any client implementation can not provide the value for a statistics, the corresponding key, valaue
 * pair will not be presented in the statistics string. Only the ones, that the client can provide will be added.
 * 
 * The statistics key names can be one of the following (Used IMap named <StatIMapName> and ICache Named
 * <StatICacheName> and assuming that the near cache is configured):
 * 
 * clientType: The string that represents the client type. See {@link com.hazelcast.core.ClientType}
 * 
 * clusterConnectionTimestamp: The time that the client connected to the cluster (milliseconds since epoch). It is reset on
 * each reconnection.
 * 
 * credentials.principal: The principal of the client if it exists. For
 * {@link com.hazelcast.security.UsernamePasswordCredentials}, this is the username, for custom authentication it is set by
 * the {@link com.hazelcast.security.Credentials} implementer.
 * 
 * clientAddress: The address of the client. It is formatted as "<IP>:<port>"
 * 
 * clientName: The name of the client instance. See ClientConfig.setInstanceName.
 * 
 * enterprise: "true" if the client is an enterprise client, "false" otherwise.
 * 
 * lastStatisticsCollectionTime: The time stamp (milliseconds since epoch) when the latest update for the statistics is
 * collected.
 * 
 * Near cache statistics (see {@link com.hazelcast.monitor.NearCacheStats}):
 * 
 * nc.<StatIMapName>.creationTime: The creation time (milliseconds since epoch) of this Near Cache on the client.
 * 
 * nc.<StatIMapName>.evictions: The number of evictions of Near Cache entries owned by this client.
 * 
 * nc.<StatIMapName>.expirations: The number of TTL and max-idle expirations of Near Cache entries owned by the client.
 * 
 * nc.<StatIMapName>.hits: The number of hits (reads) of Near Cache entries owned by the client.
 * 
 * nc.<StatIMapName>.lastPersistenceDuration: The duration in milliseconds of the last Near Cache key persistence
 * (when the pre-load feature is enabled).
 * 
 * nc.<StatIMapName>.lastPersistenceFailure: The failure reason of the last Near Cache persistence (when the pre-load
 * feature is enabled).
 * 
 * nc.<StatIMapName>.lastPersistenceKeyCount: The number of Near Cache key persistences (when the pre-load feature is
 * enabled).
 * 
 * nc.<StatIMapName>.lastPersistenceTime: The timestamp (milliseconds since epoch) of the last Near Cache key
 * persistence (when the pre-load feature is enabled).
 * 
 * nc.<StatIMapName>.lastPersistenceWrittenBytes: The written number of bytes of the last Near Cache key persistence
 * (when the pre-load feature is enabled).
 * 
 * nc.<StatIMapName>.misses: The number of misses of Near Cache entries owned by the client.
 * 
 * nc.<StatIMapName>.ownedEntryCount: the number of Near Cache entries owned by the client.
 * 
 * nc.<StatIMapName>.ownedEntryMemoryCost: Memory cost (number of bytes) of Near Cache entries owned by the client.
 * 
 * nc.hz/<StatICacheName>.creationTime: The creation time of this Near Cache on the client.
 * 
 * nc.hz/<StatICacheName>.evictions: The number of evictions of Near Cache entries owned by the client.
 * 
 * nc.hz/<StatICacheName>.expirations: The number of TTL and max-idle expirations of Near Cache entries owned by the
 * client.
 * 
 * nc.hz/<StatICacheName>.hits
 * nc.hz/<StatICacheName>.lastPersistenceDuration
 * nc.hz/<StatICacheName>.lastPersistenceFailure
 * nc.hz/<StatICacheName>.lastPersistenceKeyCount
 * nc.hz/<StatICacheName>.lastPersistenceTime
 * nc.hz/<StatICacheName>.lastPersistenceWrittenBytes
 * nc.hz/<StatICacheName>.misses
 * nc.hz/<StatICacheName>.ownedEntryCount
 * nc.hz/<StatICacheName>.ownedEntryMemoryCost
 * 
 * Operating System Statistics (see {@link com.hazelcast.internal.metrics.metricsets.OperatingSystemMetricSet},
 * {@link sun.management.OperatingSystemImpl}) and {@link com.sun.management.UnixOperatingSystemMXBean}:
 * 
 * os.committedVirtualMemorySize: The amount of virtual memory that is guaranteed to be available to the running process in
 * bytes, or -1 if this operation is not supported.
 * 
 * os.freePhysicalMemorySize: The amount of free physical memory in bytes.
 * 
 * os.freeSwapSpaceSize: The amount of free swap space in bytes.
 * 
 * os.maxFileDescriptorCount: The maximum number of file descriptors.
 * 
 * os.openFileDescriptorCount: The number of open file descriptors.
 * 
 * os.processCpuTime: The CPU time used by the process in nanoseconds.
 * 
 * os.systemLoadAverage: The system load average for the last minute. (See
 * {@link java.lang.management.OperatingSystemMXBean#getSystemLoadAverage})
 * The system load average is the sum of the number of runnable entities
 * queued to the {@link java.lang.management.OperatingSystemMXBean#getAvailableProcessors} available processors
 * and the number of runnable entities running on the available processors
 * averaged over a period of time.
 * The way in which the load average is calculated is operating system
 * specific but is typically a damped time-dependent average.
 * <p>
 * If the load average is not available, a negative value is returned.
 * <p>
 * 
 * os.totalPhysicalMemorySize: The total amount of physical memory in bytes.
 * 
 * os.totalSwapSpaceSize: The total amount of swap space in bytes.
 * 
 * Runtime statistics (See {@link Runtime}:
 * 
 * runtime.availableProcessors: The number of processors available to the process.
 * 
 * runtime.freeMemory: an approximation to the total amount of memory currently available for future allocated objects,
 * measured in bytes.
 * 
 * runtime.maxMemory: The maximum amount of memory that the process will  attempt to use, measured in bytes
 * 
 * runtime.totalMemory: The total amount of memory currently available for current and future objects, measured in bytes.
 * 
 * runtime.uptime: The uptime of the process in milliseconds.
 * 
 * runtime.usedMemory: The difference of total memory and used memory in bytes.
 * 
 * userExecutor.queueSize: The number of waiting tasks in the client user executor (See ClientExecutionService#getUserExecutor)
 * 
 * Not: Please observe that the name for the ICache appears to be the hazelcast instance name "hz" followed by "/" and
 * followed by the cache name provided which is StatICacheName.
 * 
 * An example stats string (IMap name: StatIMapName and ICache name: StatICacheName with near-cache enabled):
 * 
 * lastStatisticsCollectionTime=1496137027173,enterprise=false,clientType=JAVA,clusterConnectionTimestamp=1496137018114,
 * clientAddress=127.0.0.1:5001,clientName=hz.client_0,executionService.userExecutorQueueSize=0,runtime.maxMemory=1065025536,
 * os.freePhysicalMemorySize=32067584,os.totalPhysicalMemorySize=17179869184,os.systemLoadAverage=249,
 * runtime.usedMemory=16235040,runtime.freeMemory=115820000,os.totalSwapSpaceSize=5368709120,runtime.availableProcessors=4,
 * runtime.uptime=13616,os.committedVirtualMemorySize=4081422336,os.maxFileDescriptorCount=10240,
 * runtime.totalMemory=132055040,os.processCpuTime=6270000000,os.openFileDescriptorCount=67,os.freeSwapSpaceSize=888406016,
 * nc.StatIMapName.creationTime=1496137021761,nc.StatIMapName.evictions=0,nc.StatIMapName.hits=1,
 * nc.StatIMapName.lastPersistenceDuration=0,nc.StatIMapName.lastPersistenceKeyCount=0,nc.StatIMapName.lastPersistenceTime=0,
 * nc.StatIMapName.lastPersistenceWrittenBytes=0,nc.StatIMapName.misses=1,nc.StatIMapName.ownedEntryCount=1,
 * nc.StatIMapName.expirations=0,nc.StatIMapName.ownedEntryMemoryCost=140,nc.hz/StatICacheName.creationTime=1496137025201,
 * nc.hz/StatICacheName.evictions=0,nc.hz/StatICacheName.hits=1,nc.hz/StatICacheName.lastPersistenceDuration=0,
 * nc.hz/StatICacheName.lastPersistenceKeyCount=0,nc.hz/StatICacheName.lastPersistenceTime=0,
 * nc.hz/StatICacheName.lastPersistenceWrittenBytes=0,nc.hz/StatICacheName.misses=1,nc.hz/StatICacheName.ownedEntryCount=1,
 * nc.hz/StatICacheName.expirations=0,nc.hz/StatICacheName.ownedEntryMemoryCost=140
 */
public final class ClientStatisticsCodec {
    //hex: 0x001000
    public static final int REQUEST_MESSAGE_TYPE = 4096;
    //hex: 0x001001
    public static final int RESPONSE_MESSAGE_TYPE = 4097;
    private static final int REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private ClientStatisticsCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * The key=value pairs separated by the ',' character
         */
        public java.lang.String stats;
    }

    public static ClientMessage encodeRequest(java.lang.String stats) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("Client.Statistics");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, stats);
        return clientMessage;
    }

    public static ClientStatisticsCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        //empty initial frame
        iterator.next();
        request.stats = StringCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {
    }

    public static ClientMessage encodeResponse() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    public static ClientStatisticsCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        return response;
    }

}
