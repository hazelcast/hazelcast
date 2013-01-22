///*
// * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.hazelcast.instance;
//
//import com.hazelcast.transaction.TransactionImpl;
//
//import javax.security.auth.Subject;
//
//public class CallContext {
//    private final int threadId;
//    private final boolean client;
//    private volatile TransactionImpl txn = null;
//    private Subject currentSubject = null;
//
//    public static final CallContext DUMMY_CLIENT = new CallContext(0, true);
//
//    public CallContext(int threadId, boolean client) {
//        this.threadId = threadId;
//        this.client = client;
//    }
//
//    public TransactionImpl getTransaction() {
//        return txn;
//    }
//
//    public void setTransaction(TransactionImpl txn) {
//        this.txn = txn;
//    }
//
//    public void finalizeTransaction() {
//        this.txn = null;
//    }
//
//    public int getThreadId() {
//        return threadId;
//    }
//
//    public boolean isClient() {
//        return client;
//    }
//
//    public void reset() {
//        this.txn = null;
//    }
//
//    public Subject getSubject() {
//        return currentSubject;
//    }
//
//    public void setSubject(Subject subject) {
//        this.currentSubject = subject;
//    }
//
//    @Override
//    public String toString() {
//        return "CallContext{" +
//                "threadId=" + threadId +
//                ", client=" + client +
//                '}';
//    }
//}
