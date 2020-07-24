<#--
// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

/**
* Parses CREATE EXTERNAL TABLE statement.
*/
SqlCreate SqlCreateExternalTable(Span span, boolean replace) :
{
    SqlParserPos startPos = span.pos();

    SqlIdentifier name;
    boolean ifNotExists = false;
    SqlNodeList columns = SqlNodeList.EMPTY;
    SqlIdentifier type;
    SqlNodeList sqlOptions = SqlNodeList.EMPTY;
    SqlNode source = null;
}
{
    <EXTERNAL> <TABLE>
    [
        <IF> <NOT> <EXISTS> { ifNotExists = true; }
    ]
    name = SimpleIdentifier()
    columns = TableColumns()
    <TYPE>
    type = SimpleIdentifier()
    [
        <OPTIONS>
        sqlOptions = SqlOptions()
    ]
    {
        return new SqlCreateExternalTable(
            name,
            columns,
            type,
            sqlOptions,
            replace,
            ifNotExists,
            startPos.plus(getPos())
        );
    }
}

SqlNodeList TableColumns():
{
    SqlParserPos pos = getPos();

    Map<String, SqlNode> columns = new LinkedHashMap<String, SqlNode>();
    SqlTableColumn column;
}
{
    [
        <LPAREN> {  pos = getPos(); }
        column = TableColumn()
        {
            columns.put(column.name(), column);
        }
        (
            <COMMA> column = TableColumn()
            {
                if (columns.putIfAbsent(column.name(), column) != null) {
                   throw SqlUtil.newContextException(getPos(),
                       ParserResource.RESOURCE.duplicateColumn(column.name()));
                }
            }
        )*
        <RPAREN>
    ]
    {
        return new SqlNodeList(columns.values(), pos.plus(getPos()));
    }
}

SqlTableColumn TableColumn() :
{
    Span span;

    SqlIdentifier name;
    SqlDataType type;
    SqlIdentifier externalName = null;

}
{
    name = SimpleIdentifier() { span = span(); }
    type = SqlDataType()
    [
        <EXTERNAL> <NAME> externalName = CompoundIdentifier()
    ]
    {
        return new SqlTableColumn(name, type, externalName, span.end(this));
    }
}

SqlDataType SqlDataType() :
{
    Span span;

    QueryDataType type;
}
{
    type = QueryDataType() { span = span(); }
    {
        return new SqlDataType(type, span.end(this));
    }
}

QueryDataType QueryDataType() :
{
    QueryDataType type;
}
{
    (
        type = NumericType()
    |
        type = CharacterType()
    |
        type = DateTimeType()
    )
    {
        return type;
    }
}

QueryDataType NumericType() :
{
    QueryDataType type;
    int precision = -1;
    int scale = -1;
}
{
    (
        <BOOLEAN> { type = QueryDataType.BOOLEAN; }
    |
        <TINYINT> { type = QueryDataType.TINYINT; }
    |
        <SMALLINT> { type = QueryDataType.SMALLINT; }
    |
        <INT> { type = QueryDataType.INT; }
    |
        <BIGINT> { type = QueryDataType.BIGINT; }
    |
        <REAL> { type = QueryDataType.REAL; }
    |
        <DOUBLE> { type = QueryDataType.DOUBLE; }
    |
        <DECIMAL> { type = QueryDataType.DECIMAL; }
    )
    {
        return type;
    }
}

QueryDataType CharacterType() :
{
    QueryDataType type;
}
{
    (
        <VARCHAR> { type = QueryDataType.VARCHAR; }
    )
    {
        return type;
    }
}

QueryDataType DateTimeType() :
{
    QueryDataType type;
    SqlIdentifier variant = null;
}
{
    (
        <TIME> { type = QueryDataType.TIME; }
    |
        <DATE> { type = QueryDataType.DATE; }
    |
        <TIMESTAMP>
        (
            <WITH> <TIME> <ZONE> { type = QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME; }
        |
            { type = QueryDataType.TIMESTAMP; }
        )
    )
    {
        return type;
    }
}

/**
* Parses OPTIONS.
*/
SqlNodeList SqlOptions():
{
    Span span;

    Map<String, SqlNode> sqlOptions = new LinkedHashMap<String, SqlNode>();
    SqlOption sqlOption;
}
{
    <LPAREN> { span = span(); }
    [
        sqlOption = SqlOption()
        {
            sqlOptions.put(sqlOption.key(), sqlOption);
        }
        (
            <COMMA> sqlOption = SqlOption()
            {
                if (sqlOptions.putIfAbsent(sqlOption.key(), sqlOption) != null) {
                    throw SqlUtil.newContextException(getPos(),
                        ParserResource.RESOURCE.duplicateOption(sqlOption.key()));
                }
            }
        )*
    ]
    <RPAREN>
    {
        return new SqlNodeList(sqlOptions.values(), span.end(this));
    }
}

SqlOption SqlOption() :
{
    Span span;

    SqlIdentifier key;
    SqlNode value;
}
{
    key = SimpleIdentifier() { span = span(); }
    value = StringLiteral()
    {
        return new SqlOption(key, value, span.end(this));
    }
}

/**
* Parses DROP EXTERNAL TABLE statement.
*/
SqlDrop SqlDropExternalTable(Span span, boolean replace) :
{
    SqlParserPos pos = span.pos();

    SqlIdentifier name;
    boolean ifExists = false;
}
{
    <EXTERNAL> <TABLE>
    [
        <IF> <EXISTS> { ifExists = true; }
    ]
    name = SimpleIdentifier()
    {
        return new SqlDropExternalTable(name, ifExists, pos.plus(getPos()));
    }
}
