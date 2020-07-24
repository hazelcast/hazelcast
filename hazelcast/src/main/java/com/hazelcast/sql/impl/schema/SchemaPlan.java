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

package com.hazelcast.sql.impl.schema;

import com.hazelcast.sql.impl.explain.QueryExplain;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.optimizer.SqlPlanType;

public interface SchemaPlan extends SqlPlan {

    @Override
    default SqlPlanType getType() {
        return SqlPlanType.SCHEMA;
    }

    @Override
    default QueryExplain getExplain() {
        // TODO: implement
        throw new UnsupportedOperationException();
    }

    // TODO: add execute(List<Object> params, long timeout, int pageSize) to SqlPlan ???
    SqlPlan execute();

    class CreateExternalTablePlan implements SchemaPlan {

        private final ExternalCatalog catalog;

        private final ExternalTable schema;

        private final boolean replace;
        private final boolean ifNotExists;

        private final SqlPlan populateTablePlan;

        public CreateExternalTablePlan(
            ExternalCatalog catalog,
            ExternalTable schema,
            boolean replace,
            boolean ifNotExists,
            SqlPlan populateTablePlan
        ) {
            this.catalog = catalog;
            this.schema = schema;
            this.replace = replace;
            this.ifNotExists = ifNotExists;
            this.populateTablePlan = populateTablePlan;
        }

        @Override
        public SqlPlan execute() {
            return catalog.createTable(schema, replace, ifNotExists) ? populateTablePlan : null;
        }
    }

    class RemoveExternalTablePlan implements SchemaPlan {

        private final ExternalCatalog catalog;

        private final String name;

        private final boolean ifExists;

        public RemoveExternalTablePlan(
            ExternalCatalog catalog,
            String name,
            boolean ifExists
        ) {
            this.catalog = catalog;
            this.name = name;
            this.ifExists = ifExists;
        }

        @Override
        public SqlPlan execute() {
            catalog.removeTable(name, ifExists);
            return null;
        }
    }
}
