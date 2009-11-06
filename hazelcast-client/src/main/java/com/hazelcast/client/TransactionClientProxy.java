/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.core.Transaction;
import com.hazelcast.impl.ClusterOperation;

public class TransactionClientProxy implements Transaction, ClientProxy{
	final ProxyHelper proxyHelper;

	public TransactionClientProxy(String name, HazelcastClient client) {
		proxyHelper = new ProxyHelper(name, client);
	}

	public void begin() throws IllegalStateException {
		proxyHelper.doOp(ClusterOperation.TRANSACTION_BEGIN, null, null);
	}

	public void commit() throws IllegalStateException {
		proxyHelper.doOp(ClusterOperation.TRANSACTION_COMMIT, null, null);
		
	}

	public int getStatus() {
		return 0;
	}

	public void rollback() throws IllegalStateException {
		proxyHelper.doOp(ClusterOperation.TRANSACTION_ROLLBACK, null, null);
	}

	public void setOutRunnable(OutRunnable out) {
		proxyHelper.setOutRunnable(out);
	}

}
