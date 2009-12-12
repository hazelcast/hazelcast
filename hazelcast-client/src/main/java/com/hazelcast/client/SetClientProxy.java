/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.client;

import java.util.Iterator;

import com.hazelcast.core.ISet;
import com.hazelcast.core.IList;
import com.hazelcast.impl.ClusterOperation;
import static com.hazelcast.client.ProxyHelper.check;

public class SetClientProxy<E> extends CollectionClientProxy<E> implements ISet<E>, ClientProxy{

	public SetClientProxy(HazelcastClient client, String name) {
		super(client, name);
	}
	
	@Override	
	public boolean add(E o) {
        check(o);
		return (Boolean)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_ADD_TO_SET, o, null);
	}
	@Override
	public boolean remove(Object o) {
        check(o);
		return (Boolean)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_REMOVE_ITEM, o, null);
	}
	@Override
	public boolean contains(Object o) {
        check(o);
		return (Boolean)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_CONTAINS, o, null);
	}


    @Override
    public boolean equals(Object o){
        if(o instanceof ISet && o!=null){
            return getName().equals(((ISet)o).getName());
        }
        else{
            return false;
        }
    }

	public String getName() {
		return name.substring(4);
	}

	public InstanceType getInstanceType() {
		return InstanceType.SET;
	}

}
