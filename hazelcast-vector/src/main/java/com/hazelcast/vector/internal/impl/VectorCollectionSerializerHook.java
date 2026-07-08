/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.vector.impl.DataSearchResult;
import com.hazelcast.vector.impl.DataVectorDocument;
import com.hazelcast.vector.impl.MultiIndexVectorValues;
import com.hazelcast.vector.impl.SearchOptionsImpl;
import com.hazelcast.vector.impl.SingleIndexVectorValues;
import com.hazelcast.vector.impl.VectorCollectionSerializerConstants;
import com.hazelcast.vector.impl.VectorDocumentImpl;
import com.hazelcast.vector.internal.impl.ops.ClearBackupOperation;
import com.hazelcast.vector.internal.impl.ops.ClearOperation;
import com.hazelcast.vector.internal.impl.ops.ClearOperationsFactory;
import com.hazelcast.vector.internal.impl.ops.DeleteBackupOperation;
import com.hazelcast.vector.internal.impl.ops.DeleteOperation;
import com.hazelcast.vector.internal.impl.ops.GetOperation;
import com.hazelcast.vector.internal.impl.ops.MergeBackupOperation;
import com.hazelcast.vector.internal.impl.ops.MergeOperation;
import com.hazelcast.vector.internal.impl.ops.MergeOperationFactory;
import com.hazelcast.vector.internal.impl.ops.OptimizeBackupOperation;
import com.hazelcast.vector.internal.impl.ops.OptimizeOperation;
import com.hazelcast.vector.internal.impl.ops.OptimizeOperationsFactory;
import com.hazelcast.vector.internal.impl.ops.PutAllBackupOperation;
import com.hazelcast.vector.internal.impl.ops.PutAllOperation;
import com.hazelcast.vector.internal.impl.ops.PutAllOperationFactory;
import com.hazelcast.vector.internal.impl.ops.PutIfAbsentOperation;
import com.hazelcast.vector.internal.impl.ops.PutOperation;
import com.hazelcast.vector.internal.impl.ops.RemoveOperation;
import com.hazelcast.vector.internal.impl.ops.ReplicationOperation;
import com.hazelcast.vector.internal.impl.ops.SearchMemberOperation;
import com.hazelcast.vector.internal.impl.ops.SearchMemberResult;
import com.hazelcast.vector.internal.impl.ops.SearchOperation;
import com.hazelcast.vector.internal.impl.ops.SearchOperationsFactory;
import com.hazelcast.vector.internal.impl.ops.SetBackupOperation;
import com.hazelcast.vector.internal.impl.ops.SetOperation;
import com.hazelcast.vector.internal.impl.ops.SizeOperation;
import com.hazelcast.vector.internal.impl.ops.SizeOperationsFactory;
import com.hazelcast.vector.internal.impl.ops.VectorEntries;
import com.hazelcast.vector.internal.impl.storage.ReplicationStateHolder;
import com.hazelcast.vector.internal.impl.storage.VectorCollectionStorage.VectorCollectionMergingEntryImpl;

import java.util.function.Supplier;

@SuppressWarnings({"ClassFanOutComplexity", "ClassDataAbstractionCoupling"})
public class VectorCollectionSerializerHook extends VectorCollectionSerializerConstants implements DataSerializerHook {

    public static final short PUT = 5;
    public static final short PUT_IF_ABSENT = 6;
    public static final short PUT_ALL = 7;
    public static final short REMOVE = 8;
    public static final short SEARCH = 9;
    public static final short GET = 10;
    public static final short SET = 11;
    public static final short DELETE = 12;
    public static final short SEARCH_RESULTS = 13;
    public static final short SEARCH_RESULT = 14;
    public static final short SEARCH_FACTORY = 17;
    public static final short ENTRIES = 18;
    public static final short PUT_ALL_FACTORY = 19;
    public static final short SEARCH_MEMBER = 20;
    public static final short SEARCH_MEMBER_RESULT = 21;
    public static final short OPTIMIZE = 22;
    public static final short OPTIMIZE_FACTORY = 23;
    public static final short CLEAR = 24;
    public static final short CLEAR_FACTORY = 25;
    public static final short REPLICATION_OPERATION = 26;
    public static final short REPLICATION_STATE_HOLDER = 27;
    public static final short SIZE = 28;
    public static final short SIZE_FACTORY = 29;
    public static final short SET_BACKUP = 30;
    public static final short OPTIMIZE_BACKUP = 31;
    public static final short DELETE_BACKUP = 32;
    public static final short CLEAR_BACKUP = 33;
    public static final short PUT_ALL_BACKUP = 34;
    public static final short VECTOR_COLLECTION_MERGING_ENTRY = 35;
    public static final short MERGE_OPERATION_FACTORY = 36;
    public static final short MERGE_OPERATION = 37;
    public static final short MERGE_BACKUP_OPERATION = 38;

    public static final short LEN = MERGE_BACKUP_OPERATION + 1;

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[LEN];
        constructors[VECTOR_DOCUMENT] = VectorDocumentImpl::new;
        constructors[DATA_VECTOR_DOCUMENT] = DataVectorDocument::new;
        constructors[SEARCH_OPTIONS] = SearchOptionsImpl::new;
        constructors[DATA_SEARCH_RESULT] = DataSearchResult::new;
        constructors[PUT] = PutOperation::new;
        constructors[PUT_IF_ABSENT] = PutIfAbsentOperation::new;
        constructors[PUT_ALL] = PutAllOperation::new;
        constructors[REMOVE] = RemoveOperation::new;
        constructors[SEARCH] = SearchOperation::new;
        constructors[GET] = GetOperation::new;
        constructors[SET] = SetOperation::new;
        constructors[DELETE] = DeleteOperation::new;
        constructors[SEARCH_RESULTS] = SearchResultsImpl::new;
        constructors[SEARCH_RESULT] = SearchResultImpl::new;
        constructors[SINGLE_VECTOR_VALUES] = SingleIndexVectorValues::new;
        constructors[MULTIPLE_VECTOR_VALUES] = MultiIndexVectorValues::new;
        constructors[SEARCH_FACTORY] = SearchOperationsFactory::new;
        constructors[ENTRIES] = VectorEntries::new;
        constructors[PUT_ALL_FACTORY] = PutAllOperationFactory::new;
        constructors[SEARCH_MEMBER] = SearchMemberOperation::new;
        constructors[SEARCH_MEMBER_RESULT] = SearchMemberResult::new;
        constructors[OPTIMIZE] = OptimizeOperation::new;
        constructors[OPTIMIZE_FACTORY] = OptimizeOperationsFactory::new;
        constructors[CLEAR] = ClearOperation::new;
        constructors[CLEAR_FACTORY] = ClearOperationsFactory::new;
        constructors[REPLICATION_OPERATION] = ReplicationOperation::new;
        constructors[REPLICATION_STATE_HOLDER] = ReplicationStateHolder::new;
        constructors[SIZE] = SizeOperation::new;
        constructors[SIZE_FACTORY] = SizeOperationsFactory::new;
        constructors[SET_BACKUP] = SetBackupOperation::new;
        constructors[OPTIMIZE_BACKUP] = OptimizeBackupOperation::new;
        constructors[DELETE_BACKUP] = DeleteBackupOperation::new;
        constructors[CLEAR_BACKUP] = ClearBackupOperation::new;
        constructors[PUT_ALL_BACKUP] = PutAllBackupOperation::new;
        constructors[VECTOR_COLLECTION_MERGING_ENTRY] = VectorCollectionMergingEntryImpl::new;
        constructors[MERGE_OPERATION_FACTORY] = MergeOperationFactory::new;
        constructors[MERGE_OPERATION] = MergeOperation::new;
        constructors[MERGE_BACKUP_OPERATION] = MergeBackupOperation::new;
        return new ArrayDataSerializableFactory(constructors);
    }
}
