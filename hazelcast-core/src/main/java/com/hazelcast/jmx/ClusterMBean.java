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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.hazelcast.config.Config;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

/**
 * The instrumentation MBean for the cluster.
 * 
 * @author Marco Ferrante, DISI - University of Genoa
 */
@JMXDescription("The Hazelcast cluster")
public class ClusterMBean extends AbstractMBean<Cluster> {

	private ObjectName name;
	private MembershipListener membershipListener;
	
	public ClusterMBean(Cluster managedObject) {
		super(managedObject);
	}

   public ObjectName getObjectName() throws Exception {
		// A JVM can host only one cluster, so names are can be hardcoded.
		// Multiple clusters for JVM (see Hazelcast issue 78) need a
		// different naming schema
	   if (name == null) {
		   name = MBeanBuilder.buildObjectName("type", "Cluster");
	   }
	   return name;
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
			getManagedObject().addMembershipListener(membershipListener);

			// Init current member list
			for (Member m : getManagedObject().getMembers()) {
				registerMember(m);
			}
		}
		catch (Exception e) {
			logger.log(Level.WARNING, "Unable to start JMX member listener MBeans", e);
		} 
	}

	@Override
	public void preDeregister() throws Exception {
		getManagedObject().removeMembershipListener(membershipListener);
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
			if (mbs.isRegistered(mbean.getObjectName())) {
				mbs.unregisterMBean(mbean.getObjectName());
			}
		}
		catch (Exception e) {
			logger.log(Level.FINE, "Unable to unregister Member MBeans", e);
		} 
	}

	@JMXAttribute("ConfigFileURL")
	@JMXDescription("The URL of the current config file")
    public String getConfigFileURL() {
    	return Config.get().getConfigurationUrl().toString();
    }
    
	@JMXAttribute("GroupName")
	@JMXDescription("The current group name")
	public String getGroupName() {
		return Config.get().getGroupName();
	}
	
	@JMXAttribute("Port")
	@JMXDescription("The network port used by multicast")
	public int getPort() {
		return Config.get().getPort();
	}
	
	@JMXAttribute("PortAutoIncrement")
	@JMXDescription("The network port is autoincremented if already in use")
	public boolean isPortAutoIncrement() {
		return Config.get().isPortAutoIncrement();
	}

	@JMXAttribute("ClusterTime")
	@JMXDescription("Current cluster time")
	public long getClusterTime() {
		return getManagedObject().getClusterTime();
	}
	
	@JMXAttribute("MemberCount")
	@JMXDescription("Current size of the cluster")
	public int getMemberCount() {
		Set<Member> members = getManagedObject().getMembers();
		
		return members.size();
	}
	
	@JMXAttribute("Members")
	@JMXDescription("List of member currently in the cluster")
	public List<String> getMembers() {
		Set<Member> members = getManagedObject().getMembers();
		
		ArrayList<String> result = new ArrayList<String>();
		for (Member m : members) {
			result.add(m.getInetAddress().getHostAddress()+ ':'  + m.getPort());
		}
		
		return result;
	}

}
