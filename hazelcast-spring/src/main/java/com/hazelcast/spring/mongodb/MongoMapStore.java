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
package com.hazelcast.spring.mongodb;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.mongodb.DBObject;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBList;
import com.mongodb.DBCursor;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.Map;
import java.util.Collection;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MongoMapStore implements MapStore, MapLoaderLifecycleSupport {

    protected static final Logger LOGGER = Logger.getLogger(MongoMapStore.class.getName());
    private String mapName;
    private MongoDBConverter converter;
    private DBCollection coll;
    private MongoTemplate mongoTemplate;

    public MongoTemplate getMongoTemplate() {
        return mongoTemplate;
    }

    public void setMongoTemplate(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public void store(Object key, Object value) {
        DBObject dbo = converter.toDBObject(value);
        dbo.put("_id", key);
        coll.save(dbo);
    }

    public void storeAll(Map map) {
        for (Map.Entry entry : (Set<Map.Entry>) map.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();
            store(key, value);
        }
    }

    public void delete(Object key) {
        DBObject dbo = new BasicDBObject();
        dbo.put("_id", key);
        coll.remove(dbo);
    }

    public void deleteAll(Collection keys) {
        BasicDBList dbo = new BasicDBList();
        for (Object key : keys) {
            dbo.add(new BasicDBObject("_id", key));
        }
        BasicDBObject dbb = new BasicDBObject("$or", dbo);
        coll.remove(dbb);
    }

    public Object load(Object key) {
        DBObject dbo = new BasicDBObject();
        dbo.put("_id", key);
        DBObject obj = coll.findOne(dbo);
        if (obj == null) {
            return null;
        }

        try {
            Class clazz = Class.forName(obj.get("_class").toString());
            return converter.toObject(clazz, obj);
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.WARNING, e.getMessage(), e);
        }
        return null;
    }

    public Map loadAll(Collection keys) {
        Map map = new HashMap();
        BasicDBList dbo = new BasicDBList();
        for (Object key : keys) {
            dbo.add(new BasicDBObject("_id", key));
        }
        BasicDBObject dbb = new BasicDBObject("$or", dbo);
        DBCursor cursor = coll.find(dbb);
        while (cursor.hasNext()) {
            try {
                DBObject obj = cursor.next();
                Class clazz = Class.forName(obj.get("_class").toString());
                map.put(obj.get("_id"), converter.toObject(clazz, obj));
            } catch (ClassNotFoundException e) {
                LOGGER.log(Level.WARNING, e.getMessage(), e);
            }
        }
        return map;
    }

    public Set loadAllKeys() {
        Set keyset = new HashSet();
        BasicDBList dbo = new BasicDBList();
        dbo.add("_id");
        DBCursor cursor = coll.find(null, dbo);
        while (cursor.hasNext()) {
            keyset.add(cursor.next().get("_id"));
        }
        return keyset;
    }

    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        if (properties.get("collection") != null) {
            this.mapName = (String) properties.get("collection");
        } else {
            this.mapName = mapName;
        }
        this.coll = mongoTemplate.getCollection(this.mapName);
        this.converter = new SpringMongoDBConverter(mongoTemplate);
    }

    public void destroy() {
    }
}
