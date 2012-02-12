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

import com.hazelcast.core.Member;

import javax.management.ObjectName;
import java.net.InetAddress;

/**
 * The instrumentation MBean for a member.
 *
 * @author Marco Ferrante, DISI - University of Genoa
 */
@JMXDescription("A member of the cluster")
public class MemberMBean extends AbstractMBean<Member> {

    private ObjectName name;

    public MemberMBean(Member managedObject, ManagementService managementService) {
        super(managedObject, managementService);
    }
//	public ObjectName getObjectName() throws Exception {
//		String memberName = "Local";
//    	if (!getManagedObject().localMember()) {
//    		// String concatenation is not a performance issue,
//    		// used only during registration
//    		memberName =  '"' + getManagedObject().getInetAddress().getHostAddress()
//    				+ ":" + getManagedObject().getPort() + '"';
//    	}
//    	return getParentName().getNested("Member", memberName).buildObjectName();
//	}

    @Override
    public ObjectNameSpec getNameSpec() {
        String memberName = "Local";
        if (!getManagedObject().localMember()) {
            // String concatenation is not a performance issue,
            // used only during registration
            memberName = '"' + getManagedObject().getInetAddress().getHostAddress()
                    + ":" + getManagedObject().getPort() + '"';
        }
        return getParentName().getNested("Member", memberName);
    }

    @JMXDescription("The network Port")
    @JMXAttribute("Port")
    public int getPort() {
        return getManagedObject().getInetSocketAddress().getPort();
    }

    @JMXAttribute("InetAddress")
    @JMXDescription("The network address")
    public InetAddress getInetAddress() {
        return getManagedObject().getInetSocketAddress().getAddress();
    }

    @JMXAttribute("SuperClient")
    @JMXDescription("The member is a superclient")
    public boolean isSuperClient() {
        return getManagedObject().isSuperClient();
    }
}
