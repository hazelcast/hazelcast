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
    Span span;

    Map<String, SqlNode> columns = new LinkedHashMap<String, SqlNode>();
    SqlTableColumn column;
}
{
    <LPAREN> { span = span(); }
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
    {
        return new SqlNodeList(columns.values(), span.end(this));
    }
}

SqlTableColumn TableColumn() :
{
    Span span;

    SqlIdentifier name;
    SqlDataType type;
}
{
    name = SimpleIdentifier() { span = span(); }
    type = SqlDataType()
    {
        return new SqlTableColumn(name, type, span.end(this));
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
        (<INTEGER> | <INT>) { type = QueryDataType.INT; }
    |
        <BIGINT> { type = QueryDataType.BIGINT; }
    |
        (<REAL> | <FLOAT>) { type = QueryDataType.REAL; }
    |
        <DOUBLE> [ <PRECISION> ] { type = QueryDataType.DOUBLE; }
    |
        (<DECIMAL> | <DEC> | <NUMERIC>)
        [
            <LPAREN>
            precision = UnsignedIntLiteral()
            [
                <COMMA>
                scale = UnsignedIntLiteral()
            ]
            <RPAREN>
        ] { type = scale > 0 ? QueryDataType.DECIMAL : QueryDataType.DECIMAL_BIG_INTEGER; }
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
        (<CHARACTER> | <CHAR>)
        (
            <VARYING> { type = QueryDataType.VARCHAR; }
        |
            { type = QueryDataType.VARCHAR_CHARACTER; }
        )
    |
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
            <WITH>
            (
                <TIME> <ZONE>
                [
                    <LPAREN> variant = SimpleIdentifier() <RPAREN>
                ]
                {
                    if (variant == null || "OFFSET_DATE_TIME".equalsIgnoreCase(variant.getSimple())) {
                        type = QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
                    } else if ("ZONED_DATE_TIME".equalsIgnoreCase(variant.getSimple())) {
                        type = QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME;
                    } else if ("CALENDAR".equalsIgnoreCase(variant.getSimple())) {
                        type = QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR;
                    } else {
                        throw SqlUtil.newContextException(getPos(),
                            ParserResource.RESOURCE.unknownTimestampVariant(variant.getSimple()));
                    }
                }
            |
                <LOCAL> <TIME> <ZONE>
                [
                    <LPAREN> variant = SimpleIdentifier() <RPAREN>
                ]
                {
                    if (variant == null || "INSTANT".equalsIgnoreCase(variant.getSimple())) {
                        type = QueryDataType.TIMESTAMP_WITH_TZ_INSTANT;
                    } else if ("DATE".equalsIgnoreCase(variant.getSimple())) {
                        type = QueryDataType.TIMESTAMP_WITH_TZ_DATE;
                    } else {
                        throw SqlUtil.newContextException(getPos(),
                            ParserResource.RESOURCE.unknownTimestampVariant(variant.getSimple()));
                    }
                }
            )
        |
            <WITHOUT> <TIME> <ZONE> { type = QueryDataType.TIMESTAMP; }
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
    SqlParserPos startPos = span.pos();

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
        return new SqlDropExternalTable(name, ifExists, startPos.plus(getPos()));
    }
}

/**
* Parses an extended INSERT statement.
*/
SqlNode SqlExtendedInsert() :
{
    Span span;
    SqlNode table;
    SqlNode source;
    List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
    SqlNodeList keywordList;
    List<SqlLiteral> extendedKeywords = new ArrayList<SqlLiteral>();
    SqlNodeList extendedKeywordList;
    SqlNodeList columns = null;
}
{
    <INSERT>
    (
        <INTO>
    |
        <OVERWRITE> {
            extendedKeywords.add(SqlExtendedInsertKeyword.OVERWRITE.symbol(getPos()));
        }
    )
    { span = span(); }
    SqlInsertKeywords(keywords) {
        keywordList = new SqlNodeList(keywords, span.addAll(keywords).pos());
        extendedKeywordList = new SqlNodeList(extendedKeywords, span.addAll(extendedKeywords).pos());
    }
    table = CompoundIdentifier()
    [
        LOOKAHEAD(2)
        { Pair<SqlNodeList, SqlNodeList> p; }
        p = ParenthesizedCompoundIdentifierList() {
            if (p.right.size() > 0) {
                table = extend(table, p.right);
            }
            if (p.left.size() > 0) {
                columns = p.left;
            }
        }
    ]
    source = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return new SqlExtendedInsert(
            table,
            source,
            keywordList,
            extendedKeywordList,
            columns,
            span.end(source)
        );
    }
}
