/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.sql.impl.type.QueryDataType;
import org.bson.Document;

import java.io.Serializable;

class ProjectionData implements Serializable {

    final String externalName;
    final Document projectionExpr;
    final int index;
    final QueryDataType type;

    ProjectionData(String externalName, Document projectionExpr, int index, QueryDataType type) {
        this.externalName = externalName;
        this.projectionExpr = projectionExpr;
        this.index = index;
        this.type = type;
    }
}
