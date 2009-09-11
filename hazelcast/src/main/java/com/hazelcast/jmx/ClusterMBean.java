/* 
 * Copyright (c) 2007-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */
package com.hazelcast.jmx;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.io.File;
import java.net.URL;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.DynamicMBean;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.Instance;
import com.hazelcast.core.InstanceEvent;
import com.hazelcast.core.InstanceListener;
import com.hazelcast.impl.Node;

/**
 * The instrumentation MBean for the cluster.
 * 
 * @author Marco Ferrante, DISI - University of Genoa
 */
@JMXDescription("Hazelcast cluster")
public class ClusterMBean extends AbstractMBean<HazelcastInstance> {

	private ObjectName name;
	private ObjectNameSpec clusterObjectNames;
	private MembershipListener membershipListener;
	
	private final Config config;
	private final Cluster cluster;
	
	private MBeanBuilder mbeanBuilder = null;
	
	public ClusterMBean(HazelcastInstance instance, Config config) {
		super(instance);
		this.config = config;
		this.cluster = instance.getCluster();
		mbeanBuilder = new MBeanBuilder();
		mbeanBuilder.setClusterName(instance.getName());
		clusterObjectNames = new ObjectNameSpec(getManagedObject().getName());
	}

//	public ObjectName getObjectName() throws Exception {
//		// A JVM can host only one cluster, so names are can be hardcoded.
//		// Multiple clusters for JVM (see Hazelcast issue 78) need a
//		// different naming schema
//		if (name == null) {
//			name = new ObjectNameSpec().buildObjectName("Cluster", getManagedObject().getName());
//			
//		}
//		return name;
//	}
	
	@Override
	public ObjectNameSpec getNameSpec() {
		return new ObjectNameSpec("Cluster", getManagedObject().getName());
	}

	public ObjectNameSpec getRootName() {
		return clusterObjectNames;
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
		}
		catch (Exception e) {
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
			
			MemberMBean mbean = new MemberMBean(member);
			mbean.setParentName(clusterObjectNames);
			if (!mbs.isRegistered(mbean.getObjectName())) {
				mbs.registerMBean(mbean, mbean.getObjectName());
			}
		}
		catch (Exception e) {
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
			MemberMBean mbean = new MemberMBean(member);
			mbean.setParentName(clusterObjectNames);
			if (mbs.isRegistered(mbean.getObjectName())) {
				mbs.unregisterMBean(mbean.getObjectName());
			}
		}
		catch (Exception e) {
			logger.log(Level.FINE, "Unable to unregister Member MBeans", e);
		} 
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
		return config.getGroupName();
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
			result.add(m.getInetAddress().getHostAddress()+ ':'  + m.getPort());
		}
		
		return result;
	}

}
