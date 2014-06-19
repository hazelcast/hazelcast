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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import org.bson.BSONObject;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.Date;
import java.util.UUID;
import java.util.regex.Pattern;


public class SpringMongoDBConverter implements MongoDBConverter {

    private static Class[] acceptedClazzArray = new Class[]{Date.class, Number.class, String.class, ObjectId.class,
            BSONObject.class, Boolean.class, Double.class, Integer.class, Long.class, Pattern.class, UUID.class, };

    private MongoTemplate mongoTemplate;

    public SpringMongoDBConverter(Mongo mongo, String dbname) {
        this.mongoTemplate = new MongoTemplate(mongo, dbname);
    }

    public SpringMongoDBConverter(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public DBObject toDBObject(Object obj) {
        DBObject dbObject = new BasicDBObject();
        Object valueWrapper = getConvertedObject(obj);
        mongoTemplate.getConverter().write(valueWrapper, dbObject);
        return dbObject;
    }

    public Object toObject(Class clazz, DBObject dbObject) {
        if (clazz.equals(ValueWrapper.class)) {
            return dbObject.get("value");
        }
        return mongoTemplate.getConverter().read(clazz, dbObject);
    }

    private static Object getConvertedObject(Object obj) {

        for (int i = 0; i < acceptedClazzArray.length; i++) {
            if (obj.getClass().isAssignableFrom(acceptedClazzArray[i])) {
                return new ValueWrapper(obj);
            }

        }
        return obj;
    }

}
