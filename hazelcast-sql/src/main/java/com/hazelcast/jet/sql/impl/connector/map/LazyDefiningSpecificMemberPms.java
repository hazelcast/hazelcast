package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestProcessorMetaSupplierContext;
import com.hazelcast.jet.impl.execution.init.Contexts;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.security.PermissionsUtil;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.EmptyRow;
import com.hazelcast.sql.impl.row.Row;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;

public class LazyDefiningSpecificMemberPms extends ProcessorMetaSupplier.SpecificMemberPms {

    // TODO: transient?
    private Integer partitionArgIndex;
    private SupplierEx<?> partitionKeyExprSupplier;

    int partitionId;
    Map<Address, int[]> partitionAssignment;

    public LazyDefiningSpecificMemberPms() {
        super();
    }

    private LazyDefiningSpecificMemberPms(ProcessorSupplier supplier, int partitionArgumentIndex) {
        this.supplier = supplier;
        this.partitionArgIndex = partitionArgumentIndex;
        this.memberAddress = null;
    }

    private LazyDefiningSpecificMemberPms(ProcessorSupplier supplier, SupplierEx<?> partitionKeyExprSupplier) {
        this.supplier = supplier;
        this.partitionKeyExprSupplier = partitionKeyExprSupplier;
        this.memberAddress = null;
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
            Object obj = partitionKeyExprSupplier.get();
            if (!(obj instanceof Expression)) {
                throw new RuntimeException("");
            }
            partitionKeyExpr = (Expression<?>) obj;
        }

        // test path
        if (context instanceof TestProcessorMetaSupplierContext) {
            TestProcessorMetaSupplierContext ctx = (TestProcessorMetaSupplierContext) context;
            this.partitionId = ctx.getNodeEngine().getPartitionService().getPartitionId(
                    partitionArgIndex != null
                            ? eec.getArgument(partitionArgIndex)
                            : Objects.requireNonNull(partitionKeyExpr).eval(null, eec));
        } else {
            assert context instanceof Contexts.MetaSupplierCtx;
            Contexts.MetaSupplierCtx ctx = (Contexts.MetaSupplierCtx) context;
            this.partitionId = ctx.nodeEngine().getPartitionService()
                    .getPartitionId(
                            partitionArgIndex != null
                                    ? eec.getArgument(partitionArgIndex)
                                    : Objects.requireNonNull(partitionKeyExpr).eval(null, eec));
        }
        partitionAssignment = context.partitionAssignment();
    }


    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        Address address = null;
        for (Entry<Address, int[]> entry : partitionAssignment.entrySet()) {
            for (int pId : entry.getValue()) {
                if (pId == partitionId) {
                    address = entry.getKey();
                    break;
                }
            }
        }
        if (address == null) {
            throw new JetException("Cluster does not contain the required member: " + memberAddress);
        }

        if (!addresses.contains(address)) {
            throw new JetException("Cluster does not contain the required member: " + memberAddress);
        }
        return addr -> supplier;
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
            @Nonnull SupplierEx<?> partitionKeyExprSupplier) {
        return new LazyDefiningSpecificMemberPms(supplier, partitionKeyExprSupplier);
    }

    public static ProcessorMetaSupplier lazyForceTotalParallelismOne(
            @Nonnull ProcessorSupplier supplier,
            @Nonnull Integer partitionArgumentIndex) {
        return new LazyDefiningSpecificMemberPms(supplier, partitionArgumentIndex);
    }
}