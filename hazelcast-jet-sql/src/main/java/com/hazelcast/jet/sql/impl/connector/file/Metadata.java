/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
