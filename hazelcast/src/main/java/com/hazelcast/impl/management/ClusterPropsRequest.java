package com.hazelcast.impl.management;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.impl.PartitionManager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: msk
 * Date: 11.12.2012
 * Time: 17:01
 */
public class ClusterPropsRequest implements ConsoleRequest {

    public ClusterPropsRequest(){

    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_CLUSTER_PROPERTIES;
    }

    public Object readResponse(DataInput in) throws IOException {
        Map<String,String> properties = new LinkedHashMap<String, String>();
        int size = in.readInt();
        String[] temp;
        for(int i = 0; i < size ; i++){
            temp = in.readUTF().split(":#");
            properties.put(temp[0],temp.length == 1 ? "" : temp[1] );
        }
        return properties;
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        Map<String,String> properties = new LinkedHashMap<String, String>();
        Runtime runtime = Runtime.getRuntime();
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        PartitionManager partitionManager = mcs.getHazelcastInstance().node.concurrentMapManager.getPartitionManager();

        properties.put("hazelcast.cl_version" , mcs.getHazelcastInstance().node.initializer.getVersion() );
        properties.put("date.cl_startTime" , Long.toString(runtimeMxBean.getStartTime()));
        properties.put("seconds.cl_upTime" , Long.toString(runtimeMxBean.getUptime()));
        properties.put("memory.cl_freeMemory" , Long.toString(runtime.freeMemory()));
        properties.put("memory.cl_totalMemory" , Long.toString(runtime.totalMemory()));
        properties.put("memory.cl_maxMemory" , Long.toString(runtime.maxMemory()));
        properties.put("data.cl_immediateTasksCount" , Long.toString(partitionManager.getImmediateTasksCount()));
        properties.put("data.cl_scheduledTasksCount" , Long.toString(partitionManager.getScheduledTasksCount()));

        dos.writeInt(properties.size());

        for (Object property : properties.keySet()) {
            dos.writeUTF((String)property + ":#" + (String)properties.get(property) );
        }

    }

    public void writeData(DataOutput out) throws IOException {

    }

    public void readData(DataInput in) throws IOException {

    }
}
