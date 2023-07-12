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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.security.PermissionsUtil;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;

/**
 * A meta-supplier that will only use the given {@code ProcessorSupplier} on a node with given partition key.
 */
public class LazyDefiningSpecificMemberPms implements ProcessorMetaSupplier, IdentifiedDataSerializable {
    int partitionId;

    private ProcessorSupplier supplier;
    private SupplierEx<Expression<?>> partitionKeyExprSupplier;
    private Map<Address, int[]> partitionAssignment;
    private Integer partitionArgIndex;

    public LazyDefiningSpecificMemberPms() {
        super();
    }

    private LazyDefiningSpecificMemberPms(ProcessorSupplier supplier, int partitionArgumentIndex) {
        this.supplier = supplier;
        this.partitionArgIndex = partitionArgumentIndex;
    }

    private LazyDefiningSpecificMemberPms(ProcessorSupplier supplier, SupplierEx<Expression<?>> partitionExprSupplier) {
        this.supplier = supplier;
        this.partitionKeyExprSupplier = partitionExprSupplier;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        PermissionsUtil.checkPermission(supplier, context);
        if (context.localParallelism() != 1) {
            throw new IllegalArgumentException(
                    "Local parallelism of " + context.localParallelism() + " was requested for a vertex that "
                            + "supports only total parallelism of 1. Local parallelism must be 1.");
        }

        ExpressionEvalContext eec = ExpressionEvalContext.from(context);
        Expression<?> partitionKeyExpr = null;
        if (partitionKeyExprSupplier != null) {
            partitionKeyExpr = partitionKeyExprSupplier.get();
        }

        this.partitionId = Util.getNodeEngine(context.hazelcastInstance()).getPartitionService().getPartitionId(
                partitionArgIndex != null
                        ? eec.getArgument(partitionArgIndex)
                        : Objects.requireNonNull(partitionKeyExpr).eval(null, eec));
        partitionAssignment = context.partitionAssignment();
    }

    @Nonnull
    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        Address address = null;
        for (Entry<Address, int[]> entry : partitionAssignment.entrySet()) {
            if (Arrays.binarySearch(entry.getValue(), partitionId) >= 0) {
                address = entry.getKey();
                break;
            }
        }
        final Address finalAddress = address;
        // ExpectNothingProcessorSupplier may be eliminated by partition pruning, if used by SQL.
        return addr -> addr.equals(finalAddress) ? supplier : new ExpectNothingProcessorSupplier();
    }

    @Override
    public int preferredLocalParallelism() {
        return 1;
    }

    @Override
    public boolean isReusable() {
        return false;
    }

    @Override
    public boolean initIsCooperative() {
        if (partitionKeyExprSupplier != null) {
            return partitionKeyExprSupplier.get().isCooperative();
        } else {
            return true;
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(supplier);
        out.writeInt(partitionArgIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        supplier = in.readObject();
        partitionArgIndex = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.LAZY_SPECIFIC_MEMBER_PROCESSOR_META_SUPPLIER;
    }

    public static ProcessorMetaSupplier lazyForceTotalParallelismOne(
            @Nonnull ProcessorSupplier supplier,
            @Nonnull SupplierEx<Expression<?>> partitionKeyExprSupplier) {
        return new LazyDefiningSpecificMemberPms(supplier, partitionKeyExprSupplier);
    }

    public static ProcessorMetaSupplier lazyForceTotalParallelismOne(
            @Nonnull ProcessorSupplier supplier,
            @Nonnull Integer partitionArgumentIndex) {
        return new LazyDefiningSpecificMemberPms(supplier, partitionArgumentIndex);
    }
}
