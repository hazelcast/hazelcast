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

import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.optimizer.SqlPlanType;

public interface SchemaPlan extends SqlPlan {

    @Override
    default SqlPlanType getType() {
        return SqlPlanType.SCHEMA;
    }

    void execute();

    class CreateMappingPlan implements SchemaPlan {

        // TODO: should it be provided from the outside to execute()?
        private final ExternalCatalog catalog;

        private final TableMapping schema;

        private final boolean replace;
        private final boolean ifNotExists;

        public CreateMappingPlan(ExternalCatalog catalog,
                                 TableMapping schema, boolean replace, boolean ifNotExists) {
            this.catalog = catalog;
            this.schema = schema;
            this.replace = replace;
            this.ifNotExists = ifNotExists;
        }

        @Override
        public void execute() {
            catalog.createTable(schema, replace, ifNotExists);
        }
    }

    class DropMappingPlan implements SchemaPlan {

        // TODO: should it be provided from the outside to execute()?
        private final ExternalCatalog catalog;

        private final String name;

        private final boolean ifExists;

        public DropMappingPlan(ExternalCatalog catalog, String name, boolean ifExists) {
            this.catalog = catalog;
            this.name = name;
            this.ifExists = ifExists;
        }

        @Override
        public void execute() {
            catalog.removeTable(name, ifExists);
        }
    }
}
