/*
* Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jca;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.TransactionalList;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.core.TransactionalSet;

import javax.annotation.Resource;
import javax.ejb.Stateful;
import javax.resource.ResourceException;
import javax.resource.cci.ConnectionFactory;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

@Stateful
public class TestBean implements ITestBean {
    private final static Logger log = Logger.getLogger(TestBean.class.getName());

    @Resource(mappedName = "HazelcastCF")
    protected ConnectionFactory connectionFactory;

    @Override
    public void insertToMap(String mapname, String key, String value) {
        HazelcastConnection hzConn = null;
        try {
            hzConn = getConnection();
            TransactionalMap<String,String> txmap = hzConn.getTransactionalMap(mapname);
            txmap.put(key, value);
        } finally {
            closeConnection(hzConn);
        }
    }

    @Override
    public String getFromMap(String mapname, String key) {
        HazelcastConnection hzConn = null;
        try {
            hzConn = getConnection();
            TransactionalMap<String,String> txmap = hzConn.getTransactionalMap(mapname);
            return txmap.get(key);
        } finally {
            closeConnection(hzConn);
        }
    }

    @Override
    public void offerToQueue(String queuename, String key) {
        HazelcastConnection hzConn = null;
        try {
            hzConn = getConnection();
            TransactionalQueue<String> txqueue = hzConn.getTransactionalQueue(queuename);
            txqueue.offer(key);
        } finally {
            closeConnection(hzConn);
        }
    }

    @Override
    public String pollFromQueue(String queuename) {
        HazelcastConnection hzConn = null;
        try {
            hzConn = getConnection();
            TransactionalQueue<String> txqueue = hzConn.getTransactionalQueue(queuename);
            return txqueue.poll();
        } finally {
            closeConnection(hzConn);
        }
    }

    @Override
    public void insertToSet(String setname, String key) {
        HazelcastConnection hzConn = null;
        try {
            hzConn = getConnection();
            TransactionalSet<String> txset = hzConn.getTransactionalSet(setname);
            txset.add(key);
        } finally {
            closeConnection(hzConn);
        }
    }

    @Override
    public boolean removeFromSet(String setname, String key) {
        HazelcastConnection hzConn = null;
        try {
            hzConn = getConnection();
            TransactionalSet<String> txset = hzConn.getTransactionalSet(setname);
            return txset.remove(key);
        } finally {
            closeConnection(hzConn);
        }
    }

    @Override
    public int getSetSize(String setname) {
        HazelcastConnection hzConn = null;
        try {
            hzConn = getConnection();
            TransactionalSet<String> txset = hzConn.getTransactionalSet(setname);
            return txset.size();
        } finally {
            closeConnection(hzConn);
        }
    }

    @Override
    public void addToList(String listname, String key) {
        HazelcastConnection hzConn = null;
        try {
            hzConn = getConnection();
            TransactionalList<String> txlist = hzConn.getTransactionalList(listname);
            txlist.add(listname);
        } finally {
            closeConnection(hzConn);
        }
    }

    @Override
    public boolean removeFromList(String listname, String key) {
        HazelcastConnection hzConn = null;
        boolean retVal = false;
        try {
            hzConn = getConnection();
            TransactionalList<String> txlist = hzConn.getTransactionalList(listname);
            retVal = txlist.remove(key);
        } finally {
            closeConnection(hzConn);
        }
        return retVal;
    }

    @Override
    public int getListSize(String listname) {
        HazelcastConnection hzConn = null;
        try {
            hzConn = getConnection();
            TransactionalList<String> txlist = hzConn.getTransactionalList(listname);
            return txlist.size();
        } finally {
            closeConnection(hzConn);
        }
    }

    @Override
    public void addDistributedObjectListener(DistributedObjectListener obj) {
        getConnection().addDistributedObjectListener(obj);
    }

    @Override
    public void removeDistributedObjectListener(String regId) {
        getConnection().removeDistributedObjectListener(regId);
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        return getConnection().getDistributedObjects();
    }

    private HazelcastConnection getConnection() {
        try {
            return (HazelcastConnection) connectionFactory.getConnection();
        } catch (ResourceException e) {
            throw new RuntimeException("Error while getting Hazelcast connection", e);
        }
    }

    private void closeConnection(HazelcastConnection hzConn) {
        if (hzConn != null) {
            try {
                hzConn.close();
            } catch (ResourceException e) {
                log.log(Level.WARNING, "Error while closing Hazelcast connection.", e);
            }
        }
    }
}