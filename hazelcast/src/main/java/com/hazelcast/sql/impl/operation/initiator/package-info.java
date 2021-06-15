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

/**
 * Contains operations sent from the initiator member to coordinator
 * members. They match the SQL client messages.
 * <p>
 * In most cases, these operations are local. We only send to another
 * member if the initiator is a lite member or a member of the smaller
 * same-version member group.
 */
package com.hazelcast.sql.impl.operation.initiator;
