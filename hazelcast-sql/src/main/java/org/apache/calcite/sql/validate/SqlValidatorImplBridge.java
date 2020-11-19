package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;

public class SqlValidatorImplBridge extends SqlValidatorImpl {

    public SqlValidatorImplBridge(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        Config config
    ) {
        super(opTab, catalogReader, typeFactory, config);
    }

    @Override
    public RelDataType deriveTypeImpl(SqlValidatorScope scope, SqlNode operand) {
        return super.deriveTypeImpl(scope, operand);
    }
}
