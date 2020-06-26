/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.connector;

import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;
import static java.util.stream.Collectors.toList;

public abstract class LocalAbstractMapConnector extends SqlKeyValueConnector {

    protected static List<TableField> mergeMapFields(Map<String, TableField> keyFields, Map<String, TableField> valueFields) {
        LinkedHashMap<String, TableField> res = new LinkedHashMap<>(keyFields);

        // Value fields do not override key fields.
        for (Map.Entry<String, TableField> valueFieldEntry : valueFields.entrySet()) {
            res.putIfAbsent(valueFieldEntry.getKey(), valueFieldEntry.getValue());
        }

        return new ArrayList<>(res.values());
    }

    protected static List<TableField> toMapFields(List<ExternalField> externalFields) {
        if (externalFields == null) {
            return null;
        }
        return externalFields.stream()
                             .map(field -> {
                                 boolean isKey = field.extName().startsWith(KEY_ATTRIBUTE_NAME.value());
                                 assert isKey || field.extName().startsWith(THIS_ATTRIBUTE_NAME.value());
                                 int dotPos = field.extName().indexOf('.');

                                 return new MapTableField(field.name(), field.type(), false,
                                         new QueryPath(dotPos < 0 ? null : field.extName().substring(dotPos + 1), isKey));
                             })
                             .collect(toList());
    }
}
