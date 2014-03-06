package com.hazelcast.management;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.monitor.impl.LocalExecutorStatsImpl;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.monitor.impl.LocalMultiMapStatsImpl;
import com.hazelcast.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.monitor.impl.MemberStateImpl;
import com.hazelcast.nio.Address;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A Factory for creating {@link com.hazelcast.monitor.TimedMemberState} instances.
 */
public class TimedMemberStateFactory {

    private final HazelcastInstanceImpl instance;
    private final int maxVisibleInstanceCount;

    public TimedMemberStateFactory(HazelcastInstanceImpl instance) {
        this.instance = instance;
        maxVisibleInstanceCount = instance.node.groupProperties.MC_MAX_INSTANCE_COUNT.getInteger();
    }

    public TimedMemberState createTimedMemberState() {
        MemberStateImpl memberState = new MemberStateImpl();
        createMemberState(memberState);
        GroupConfig groupConfig = instance.getConfig().getGroupConfig();
        TimedMemberState timedMemberState = new TimedMemberState();
        timedMemberState.setMaster(instance.node.isMaster());
        if (timedMemberState.getMaster()) {
            timedMemberState.setMemberList(new ArrayList<String>());
            Set<Member> memberSet = instance.getCluster().getMembers();
            for (Member member : memberSet) {
                MemberImpl memberImpl = (MemberImpl) member;
                Address address = memberImpl.getAddress();
                timedMemberState.getMemberList().add(address.getHost() + ":" + address.getPort());
            }
        }
        timedMemberState.setMemberState(memberState);
        timedMemberState.setClusterName(groupConfig.getName());
        timedMemberState.setInstanceNames(getLongInstanceNames());
        return timedMemberState;
    }

    private void createMemberState(MemberStateImpl memberState) {
        final Node node = instance.node;
        memberState.setAddress(node.getThisAddress());
        PartitionService partitionService = instance.getPartitionService();
        Set<Partition> partitions = partitionService.getPartitions();
        memberState.clearPartitions();
        for (Partition partition : partitions) {
            if (partition.getOwner() != null && partition.getOwner().localMember()) {
                memberState.addPartition(partition.getPartitionId());
            }
        }
        Collection<DistributedObject> proxyObjects = new ArrayList<DistributedObject>(instance.getDistributedObjects());
        createRuntimeProps(memberState);
        createMemState(memberState, proxyObjects);
    }

    private void createRuntimeProps(MemberStateImpl memberState) {
        Runtime runtime = Runtime.getRuntime();
        ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        ClassLoadingMXBean clMxBean = ManagementFactory.getClassLoadingMXBean();
        MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemory = memoryMxBean.getHeapMemoryUsage();
        MemoryUsage nonHeapMemory = memoryMxBean.getNonHeapMemoryUsage();
        Map<String, Long> map = new HashMap<String, Long>();
        map.put("runtime.availableProcessors", Integer.valueOf(runtime.availableProcessors()).longValue());
        map.put("date.startTime", runtimeMxBean.getStartTime());
        map.put("seconds.upTime", runtimeMxBean.getUptime());
        map.put("memory.maxMemory", runtime.maxMemory());
        map.put("memory.freeMemory", runtime.freeMemory());
        map.put("memory.totalMemory", runtime.totalMemory());
        map.put("memory.heapMemoryMax", heapMemory.getMax());
        map.put("memory.heapMemoryUsed", heapMemory.getUsed());
        map.put("memory.nonHeapMemoryMax", nonHeapMemory.getMax());
        map.put("memory.nonHeapMemoryUsed", nonHeapMemory.getUsed());
        map.put("runtime.totalLoadedClassCount", clMxBean.getTotalLoadedClassCount());
        map.put("runtime.loadedClassCount", Integer.valueOf(clMxBean.getLoadedClassCount()).longValue());
        map.put("runtime.unloadedClassCount", clMxBean.getUnloadedClassCount());
        map.put("runtime.totalStartedThreadCount", threadMxBean.getTotalStartedThreadCount());
        map.put("runtime.threadCount", Integer.valueOf(threadMxBean.getThreadCount()).longValue());
        map.put("runtime.peakThreadCount", Integer.valueOf(threadMxBean.getPeakThreadCount()).longValue());
        map.put("runtime.daemonThreadCount", Integer.valueOf(threadMxBean.getDaemonThreadCount()).longValue());

        OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
        map.put("osMemory.freePhysicalMemory", get(osMxBean, "getFreePhysicalMemorySize", 0L));
        map.put("osMemory.committedVirtualMemory", get(osMxBean, "getCommittedVirtualMemorySize", 0L));
        map.put("osMemory.totalPhysicalMemory", get(osMxBean, "getTotalPhysicalMemorySize", 0L));

        map.put("osSwap.freeSwapSpace", get(osMxBean, "getFreeSwapSpaceSize", 0L));
        map.put("osSwap.totalSwapSpace", get(osMxBean, "getTotalSwapSpaceSize", 0L));
        map.put("os.maxFileDescriptorCount", get(osMxBean, "getMaxFileDescriptorCount", 0L));
        map.put("os.openFileDescriptorCount", get(osMxBean, "getOpenFileDescriptorCount", 0L));
        map.put("os.processCpuLoad", get(osMxBean, "getProcessCpuLoad", -1L));
        map.put("os.systemLoadAverage", get(osMxBean, "getSystemLoadAverage", -1L));
        map.put("os.systemCpuLoad", get(osMxBean, "getSystemCpuLoad", -1L));
        map.put("os.processCpuTime", get(osMxBean, "getProcessCpuTime", 0L));

        map.put("os.availableProcessors", get(osMxBean, "getAvailableProcessors", 0L));

        memberState.setRuntimeProps(map);
    }

    private static Long get(OperatingSystemMXBean mbean, String methodName, Long defaultValue) {
        try {
            Method method = mbean.getClass().getMethod(methodName);
            method.setAccessible(true);

            Object value = method.invoke(mbean);
            if (value == null) {
                return defaultValue;
            }

            if (value instanceof Integer) {
                return (long) (Integer) value;
            }

            if (value instanceof Double) {
                double v = (Double) value;
                return Math.round(v * 100);
            }

            if (value instanceof Long) {
                return (Long) value;
            }

            return defaultValue;
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private void createMemState(MemberStateImpl memberState,
                                Collection<DistributedObject> distributedObjects) {
        int count = 0;
        final Config config = instance.getConfig();
        for (DistributedObject distributedObject : distributedObjects) {
            if (count < maxVisibleInstanceCount) {
                if (distributedObject instanceof IMap) {
                    IMap map = (IMap) distributedObject;
                    if (config.findMapConfig(map.getName()).isStatisticsEnabled()) {
                        memberState.putLocalMapStats(map.getName(), (LocalMapStatsImpl) map.getLocalMapStats());
                        count++;
                    }
                } else if (distributedObject instanceof IQueue) {
                    IQueue queue = (IQueue) distributedObject;
                    if (config.findQueueConfig(queue.getName()).isStatisticsEnabled()) {
                        LocalQueueStatsImpl stats = (LocalQueueStatsImpl) queue.getLocalQueueStats();
                        memberState.putLocalQueueStats(queue.getName(), stats);
                        count++;
                    }
                } else if (distributedObject instanceof ITopic) {
                    ITopic topic = (ITopic) distributedObject;
                    if (config.findTopicConfig(topic.getName()).isStatisticsEnabled()) {
                        LocalTopicStatsImpl stats = (LocalTopicStatsImpl) topic.getLocalTopicStats();
                        memberState.putLocalTopicStats(topic.getName(), stats);
                        count++;
                    }
                } else if (distributedObject instanceof MultiMap) {
                    MultiMap multiMap = (MultiMap) distributedObject;
                    if (config.findMultiMapConfig(multiMap.getName()).isStatisticsEnabled()) {
                        LocalMultiMapStatsImpl stats = (LocalMultiMapStatsImpl) multiMap.getLocalMultiMapStats();
                        memberState.putLocalMultiMapStats(multiMap.getName(), stats);
                        count++;
                    }
                } else if (distributedObject instanceof IExecutorService) {
                    IExecutorService executorService = (IExecutorService) distributedObject;
                    if (config.findExecutorConfig(executorService.getName()).isStatisticsEnabled()) {
                        LocalExecutorStatsImpl stats = (LocalExecutorStatsImpl) executorService.getLocalExecutorStats();
                        memberState.putLocalExecutorStats(executorService.getName(), stats);
                        count++;
                    }
                }
            }
        }
    }

    private Set<String> getLongInstanceNames() {
        Set<String> setLongInstanceNames = new HashSet<String>(maxVisibleInstanceCount);
        Collection<DistributedObject> proxyObjects = new ArrayList<DistributedObject>(instance.getDistributedObjects());
        collectInstanceNames(setLongInstanceNames, proxyObjects);
        return setLongInstanceNames;
    }

    private void collectInstanceNames(Set<String> setLongInstanceNames,
                                      Collection<DistributedObject> distributedObjects) {
        int count = 0;
        final Config config = instance.getConfig();
        for (DistributedObject distributedObject : distributedObjects) {
            if (count < maxVisibleInstanceCount) {
                if (distributedObject instanceof MultiMap) {
                    MultiMap multiMap = (MultiMap) distributedObject;
                    if (config.findMultiMapConfig(multiMap.getName()).isStatisticsEnabled()) {
                        setLongInstanceNames.add("m:" + multiMap.getName());
                        count++;
                    }
                } else if (distributedObject instanceof IMap) {
                    IMap map = (IMap) distributedObject;
                    if (config.findMapConfig(map.getName()).isStatisticsEnabled()) {
                        setLongInstanceNames.add("c:" + map.getName());
                        count++;
                    }
                } else if (distributedObject instanceof IQueue) {
                    IQueue queue = (IQueue) distributedObject;
                    if (config.findQueueConfig(queue.getName()).isStatisticsEnabled()) {
                        setLongInstanceNames.add("q:" + queue.getName());
                        count++;
                    }
                } else if (distributedObject instanceof ITopic) {
                    ITopic topic = (ITopic) distributedObject;
                    if (config.findTopicConfig(topic.getName()).isStatisticsEnabled()) {
                        setLongInstanceNames.add("t:" + topic.getName());
                        count++;
                    }
                } else if (distributedObject instanceof IExecutorService) {
                    IExecutorService executorService = (IExecutorService) distributedObject;
                    if (config.findExecutorConfig(executorService.getName()).isStatisticsEnabled()) {
                        setLongInstanceNames.add("e:" + executorService.getName());
                        count++;
                    }
                }
            }
        }
    }
}
