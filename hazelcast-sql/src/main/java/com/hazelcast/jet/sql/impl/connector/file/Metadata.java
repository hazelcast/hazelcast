/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.List;

import static java.util.Objects.requireNonNull;

class Metadata {

    private final List<TableField> fields;
    private final ProcessorMetaSupplierProvider processorMetaSupplierProvider;
    private final SupplierEx<QueryTarget> queryTargetSupplier;

    Metadata(
            List<TableField> fields,
            ProcessorMetaSupplierProvider processorMetaSupplierProvider,
            SupplierEx<QueryTarget> queryTargetSupplier
    ) {
        this.fields = requireNonNull(fields);
        this.processorMetaSupplierProvider = requireNonNull(processorMetaSupplierProvider);
        this.queryTargetSupplier = requireNonNull(queryTargetSupplier);
    }

    List<TableField> fields() {
        return fields;
    }

    ProcessorMetaSupplierProvider processorMetaSupplier() {
        return processorMetaSupplierProvider;
    }

    SupplierEx<QueryTarget> queryTargetSupplier() {
        return queryTargetSupplier;
    }
}
