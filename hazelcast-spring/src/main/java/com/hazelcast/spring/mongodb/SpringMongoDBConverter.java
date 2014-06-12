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

    private MongoTemplate mongoTemplate;

    public SpringMongoDBConverter(Mongo mongo, String dbname) {
        this.mongoTemplate = new MongoTemplate(mongo, dbname);
    }

    public SpringMongoDBConverter(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public DBObject toDBObject(Object obj) {
        DBObject dbObject = new BasicDBObject();
        if (isStandardClass(obj.getClass())) {
            obj = new ValueWrapper(obj);
        }
        mongoTemplate.getConverter().write(obj, dbObject);
        return dbObject;
    }

    public Object toObject(Class clazz, DBObject dbObject) {
        if (clazz.equals(ValueWrapper.class)) {
            return dbObject.get("value");
        }
        return mongoTemplate.getConverter().read(clazz, dbObject);
    }

    public static boolean isStandardClass(Class clazz) {
        if (clazz.isAssignableFrom(Date.class)) {
            // standard, pass
            return true;
        } else if (clazz.isAssignableFrom(Number.class)) {
            // standard, pass
            return true;
        } else if (clazz.isAssignableFrom(String.class)) {
            // standard, pass
            return true;
        } else if (clazz.isAssignableFrom(ObjectId.class)) {
            // standard, pass
            return true;
        } else if (clazz.isAssignableFrom(BSONObject.class)) {
            // standard, pass
            return true;
        } else if (clazz.isAssignableFrom(Boolean.class)) {
            // standard, pass
            return true;
        } else if (clazz.isAssignableFrom(Double.class)) {
            // standard, pass
            return true;
        } else if (clazz.isAssignableFrom(Integer.class)) {
            // standard, pass
            return true;
        } else if (clazz.isAssignableFrom(Long.class)) {
            // standard, pass
            return true;
        } else if (clazz.isAssignableFrom(Pattern.class)) {
            // standard, pass
            return true;
        } else if (clazz.isAssignableFrom(UUID.class)) {
            // standard, pass
            return true;
        }

        return false;
    }

}
