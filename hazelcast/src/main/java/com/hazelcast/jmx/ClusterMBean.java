/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.jmx;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;

import javax.management.MBeanServer;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

/**
 * The instrumentation MBean for the cluster.
 *
 * @author Marco Ferrante, DISI - University of Genoa
 */
@JMXDescription("Hazelcast cluster")
public class ClusterMBean extends AbstractMBean<HazelcastInstance> {

    private ObjectNameSpec clusterObjectNames;
    private MembershipListener membershipListener;

    private final Config config;
    private final Cluster cluster;
    private final String name;

    public ClusterMBean(ManagementService service, String name) {
        super(service.getInstance(), service);
        this.name = name;
        this.config = service.getInstance().getConfig();
        this.cluster = service.getInstance().getCluster();
        clusterObjectNames = new ObjectNameSpec(name);
    }

    @Override
    public ObjectNameSpec getNameSpec() {
        return new ObjectNameSpec("Cluster", name);
    }

    public ObjectNameSpec getRootName() {
        return clusterObjectNames;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public void postRegister(Boolean registrationDone) {
        super.postRegister(registrationDone);
        if (!registrationDone) {
            return;
        }
        // Register the members
        try {
            // Keep member list up-to-date
            membershipListener = new MembershipListener() {

                public void memberAdded(MembershipEvent event) {
                    registerMember(event.getMember());
                }

                public void memberRemoved(MembershipEvent event) {
                    unregisterMember(event.getMember());
                }
            };
            cluster.addMembershipListener(membershipListener);
            // Init current member list
            for (Member m : cluster.getMembers()) {
                registerMember(m);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Unable to start JMX member listener MBeans", e);
        }
    }

    @Override
    public void preDeregister() throws Exception {
        cluster.removeMembershipListener(membershipListener);
        super.preDeregister();
    }

    /**
     * Register a MBean for a member
     *
     * @param member
     */
    private void registerMember(Member member) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            MemberMBean mbean = new MemberMBean(member, managementService);
            mbean.setParentName(clusterObjectNames);
            if (!mbs.isRegistered(mbean.getObjectName())) {
                mbs.registerMBean(mbean, mbean.getObjectName());
            }
        } catch (Exception e) {
            logger.log(Level.FINE, "Unable to register Member MBeans", e);
        }
    }

    /**
     * Remove registration of a member MBean
     *
     * @param member
     */
    private void unregisterMember(Member member) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            MemberMBean mbean = new MemberMBean(member, managementService);
            mbean.setParentName(clusterObjectNames);
            if (mbs.isRegistered(mbean.getObjectName())) {
                mbs.unregisterMBean(mbean.getObjectName());
            }
        } catch (Exception e) {
            logger.log(Level.FINE, "Unable to unregister Member MBeans", e);
        }
    }

    @JMXAttribute("Config")
    @JMXDescription("Config.toString() result")
    public String getConfig() {
        return config.toString();
    }

    @JMXAttribute("InstanceName")
    @JMXDescription("Name of intance")
    public String getInstanceName() {
        return getManagedObject().getName();
    }

    @JMXAttribute("ConfigSource")
    @JMXDescription("The source of the cluster instance configuration")
    public String getConfigFileURL() {
        File configurationFile = config.getConfigurationFile();
        if (configurationFile != null) {
            return configurationFile.getAbsolutePath();
        }
        URL configurationUrl = config.getConfigurationUrl();
        if (configurationUrl != null) {
            return configurationUrl.toString();
        }
        return null;
    }

    @JMXAttribute("GroupName")
    @JMXDescription("The current group name")
    public String getGroupName() {
        return config.getGroupConfig().getName();
    }

    @JMXAttribute("Port")
    @JMXDescription("The network port used by multicast")
    public int getPort() {
        return config.getPort();
    }

    @JMXAttribute("PortAutoIncrement")
    @JMXDescription("The network port is autoincremented if already in use")
    public boolean isPortAutoIncrement() {
        return config.isPortAutoIncrement();
    }

    @JMXAttribute("ClusterTime")
    @JMXDescription("Current cluster time")
    public long getClusterTime() {
        return cluster.getClusterTime();
    }

    @JMXAttribute("MemberCount")
    @JMXDescription("Current size of the cluster")
    public int getMemberCount() {
        Set<Member> members = cluster.getMembers();
        return members.size();
    }

    @JMXAttribute("Members")
    @JMXDescription("List of member currently in the cluster")
    public List<String> getMembers() {
        Set<Member> members = cluster.getMembers();
        ArrayList<String> result = new ArrayList<String>();
        for (Member m : members) {
            final InetSocketAddress socketAddress = m.getInetSocketAddress();
            result.add(socketAddress.getHostName() + ':' + socketAddress.getPort());
        }
        return result;
    }

    @JMXAttribute("Running")
    @JMXDescription("Node's running state")
    public boolean isRunning() {
        final LifecycleService lifecycleService = getManagedObject().getLifecycleService();
        return lifecycleService.isRunning();
    }

    @JMXOperation("restart")
    @JMXDescription("Restart node")
    public void restart() {
        final LifecycleService lifecycleService = getManagedObject().getLifecycleService();
        lifecycleService.restart();
    }

    @JMXOperation("shutdown")
    @JMXDescription("Shutdown node")
    public void shutdown() {
        final LifecycleService lifecycleService = getManagedObject().getLifecycleService();
        lifecycleService.shutdown();
    }
}
