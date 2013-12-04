/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.security.permission;

import com.hazelcast.config.Config;

import java.security.Permission;

/**
 * @TODO  Object Permission
 */
public abstract class InstancePermission extends ClusterPermission {
	
	protected final static int NONE 		= 0x0;
	protected final static int CREATE 		= 0x1;
	protected final static int DESTROY 		= 0x2;
	
	protected final int mask;
	protected final String actions;

	public InstancePermission(String name) {
		this(name, new String[0]);
	}
	
	public InstancePermission(String name, String... actions) {
		super(name);
		if(name == null || "".equals(name)) {
			throw new IllegalArgumentException("Permission name is mamdatory!");
		}
		mask = initMask(actions);
		
		final StringBuilder s = new StringBuilder();
		for (int i = 0; i < actions.length; i++) {
			s.append(actions[i]).append(" ");
		}
		this.actions = s.toString();
	}
	
	/**
	 * init mask
	 */
	protected abstract int initMask(String[] actions); 

	public boolean implies(Permission permission) {
		if(this.getClass() != permission.getClass()) {
			return false;
		}
		
		InstancePermission that = (InstancePermission) permission;
		
		boolean maskTest = ((this.mask & that.mask) == that.mask);
		if(!maskTest) {
			return false;
		}
		
		if(!Config.nameMatches(that.getName(), this.getName())) {
			return false;
		}
		
		return true;
	}
	
	public String getActions() {
		return actions;
	}
	
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		InstancePermission other = (InstancePermission) obj;
		if(getName() == null && other.getName() != null){
			return false;
		}
		if(!getName().equals(other.getName())) {
			return false;
		}
		if(mask != other.mask) {
			return false;
		}
		return true;
	}
}
