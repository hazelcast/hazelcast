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

package com.hazelcast.client.util;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.TransactionClientProxy;
import com.hazelcast.core.Transaction;
import com.hazelcast.instance.ThreadContext;

/**
 * @mdogan 5/23/12
 */
public class TransactionUtil {

    public static Transaction getTransaction(HazelcastClient client) {
//        final ThreadContext ctx = ThreadContext.get();
//        TransactionClientProxy transactionProxy = ctx.getAttachment();
//        if (transactionProxy == null) {
//            transactionProxy = new TransactionClientProxy(null, client);
//            ctx.setAttachment(transactionProxy);
//        }
//        return transactionProxy;
        //TODO: Need to fix this. Commented because it didn't compile.
        return null;
    }

    public static void removeTransaction() {
//        ThreadContext.get().setAttachment(null);
    }
}
