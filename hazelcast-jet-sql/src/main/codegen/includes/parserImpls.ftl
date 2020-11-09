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
 * Parses CREATE EXTERNAL MAPPING statement.
 */
SqlCreate SqlCreateMapping(Span span, boolean replace) :
{
    SqlParserPos startPos = span.pos();

    SqlIdentifier name;
    SqlNodeList columns = SqlNodeList.EMPTY;
    SqlIdentifier type;
    SqlNodeList sqlOptions = SqlNodeList.EMPTY;
    boolean ifNotExists = false;
}
{
    [ <EXTERNAL> ] <MAPPING>
    [
        <IF> <NOT> <EXISTS> { ifNotExists = true; }
    ]
    name = CompoundIdentifier()
    columns = MappingColumns()
    <TYPE>
    type = SimpleIdentifier()
    [
        <OPTIONS>
        sqlOptions = SqlOptions()
    ]
    {
        return new SqlCreateMapping(
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

SqlNodeList MappingColumns():
{
    SqlParserPos pos = getPos();

    SqlMappingColumn column;
    Map<String, SqlNode> columns = new LinkedHashMap<String, SqlNode>();
}
{
    [
        <LPAREN> {  pos = getPos(); }
        column = MappingColumn()
        {
            columns.put(column.name(), column);
        }
        (
            <COMMA> column = MappingColumn()
            {
                if (columns.putIfAbsent(column.name(), column) != null) {
                   throw SqlUtil.newContextException(getPos(), ParserResource.RESOURCE.duplicateColumn(column.name()));
                }
            }
        )*
        <RPAREN>
    ]
    {
        return new SqlNodeList(columns.values(), pos.plus(getPos()));
    }
}

SqlMappingColumn MappingColumn() :
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
        <EXTERNAL> <NAME> externalName = SimpleIdentifier()
    ]
    {
        return new SqlMappingColumn(name, type, externalName, span.end(this));
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
        type = NumericTypes()
    |
        type = CharacterTypes()
    |
        type = DateTimeTypes()
    |
        type = ObjectTypes()
    )
    {
        return type;
    }
}

QueryDataType NumericTypes() :
{
    QueryDataType type;
}
{
    (
        <BOOLEAN> { type = QueryDataType.BOOLEAN; }
    |
        <TINYINT> { type = QueryDataType.TINYINT; }
    |
        <SMALLINT> { type = QueryDataType.SMALLINT; }
    |
        ( <INT> | <INTEGER> ) { type = QueryDataType.INT; }
    |
        <BIGINT> { type = QueryDataType.BIGINT; }
    |
        <REAL> { type = QueryDataType.REAL; }
    |
        <DOUBLE> [ <PRECISION> ] { type = QueryDataType.DOUBLE; }
    |
        (<DECIMAL> | <DEC> | <NUMERIC> ) { type = QueryDataType.DECIMAL; }
    )
    {
        return type;
    }
}

QueryDataType CharacterTypes() :
{
    QueryDataType type;
}
{
        ( <VARCHAR> | ( <CHAR> | <CHARACTER> ) <VARYING> ) { type = QueryDataType.VARCHAR; }
    {
        return type;
    }
}

QueryDataType DateTimeTypes() :
{
    QueryDataType type;
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

QueryDataType ObjectTypes() :
{
    QueryDataType type;
}
{
    <OBJECT> { type = QueryDataType.OBJECT; }
    {
        return type;
    }
}

/**
 * Parses DROP EXTERNAL MAPPING statement.
 */
SqlDrop SqlDropMapping(Span span, boolean replace) :
{
    SqlParserPos pos = span.pos();

    SqlIdentifier name;
    boolean ifExists = false;
}
{
    [ <EXTERNAL> ] <MAPPING>
    [
        <IF> <EXISTS> { ifExists = true; }
    ]
    name = CompoundIdentifier()
    {
        return new SqlDropMapping(name, ifExists, pos.plus(getPos()));
    }
}

/**
 * Parses CREATE JOB statement.
 */
SqlCreate SqlCreateJob(Span span, boolean replace) :
{
    SqlParserPos startPos = span.pos();

    SqlIdentifier name;
    boolean ifNotExists = false;
    SqlNodeList sqlOptions = SqlNodeList.EMPTY;
    SqlExtendedInsert sqlInsert = null;

    if (replace) {
        throw SqlUtil.newContextException(getPos(), ParserResource.RESOURCE.notSupported("OR REPLACE", "CREATE JOB"));
    }
}
{
    <JOB>
    [
        <IF> <NOT> <EXISTS> { ifNotExists = true; }
    ]
    name = SimpleIdentifier()
    [
        <OPTIONS>
        sqlOptions = SqlOptions()
    ]
    <AS>
    sqlInsert = SqlExtendedInsert()
    {
        return new SqlCreateJob(
            name,
            sqlOptions,
            sqlInsert,
            ifNotExists,
            startPos.plus(getPos())
        );
    }
}

/**
 * Parses ALTER JOB statement.
 */
SqlAlterJob SqlAlterJob() :
{
    SqlParserPos pos = getPos();

    SqlIdentifier name;
    SqlAlterJob.AlterJobOperation operation;
}
{
    <ALTER> <JOB>
    name = SimpleIdentifier()
    (
        <SUSPEND> {
            operation = SqlAlterJob.AlterJobOperation.SUSPEND;
        }
    |
        <RESUME> {
            operation = SqlAlterJob.AlterJobOperation.RESUME;
        }
    |
        <RESTART> {
            operation = SqlAlterJob.AlterJobOperation.RESTART;
        }
    )
    {
        return new SqlAlterJob(name, operation, pos.plus(getPos()));
    }
}

/**
 * Parses DROP JOB statement.
 */
SqlDrop SqlDropJob(Span span, boolean replace) :
{
    SqlParserPos pos = span.pos();

    SqlIdentifier name;
    boolean ifExists = false;
    SqlIdentifier withSnapshotName = null;
}
{
    <JOB>
    [
        <IF> <EXISTS> { ifExists = true; }
    ]
    name = SimpleIdentifier()
    [
        <WITH> <SNAPSHOT>
        withSnapshotName = SimpleIdentifier()
    ]
    {
        return new SqlDropJob(name, ifExists, withSnapshotName, pos.plus(getPos()));
    }
}

/**
 * Parses CREATE SNAPSHOT statement
 */
SqlCreate SqlCreateSnapshot(Span span, boolean replace) :
{
    SqlParserPos startPos = span.pos();
    SqlIdentifier snapshotName;
    SqlIdentifier jobName;
}
{
    <SNAPSHOT>
    snapshotName = SimpleIdentifier()
    <FOR> <JOB>
    jobName = SimpleIdentifier()
    {
        return new SqlCreateSnapshot(
            snapshotName,
            jobName,
            replace,
            startPos.plus(getPos())
        );
    }
}

/**
 * Parses DROP SNAPSHOT statement
 */
SqlDrop SqlDropSnapshot(Span span, boolean replace) :
{
    SqlParserPos pos = span.pos();

    SqlIdentifier name;
    boolean ifExists = false;
}
{
    <SNAPSHOT>
    [
        <IF> <EXISTS> { ifExists = true; }
    ]
    name = SimpleIdentifier()
    {
        return new SqlDropSnapshot(name, ifExists, pos.plus(getPos()));
    }
}

/**
 * Parses OPTIONS.
 */
SqlNodeList SqlOptions():
{
    Span span;

    SqlOption sqlOption;
    Map<String, SqlNode> sqlOptions = new LinkedHashMap<String, SqlNode>();
}
{
    <LPAREN> { span = span(); }
    [
        sqlOption = SqlOption()
        {
            sqlOptions.put(sqlOption.keyString(), sqlOption);
        }
        (
            <COMMA> sqlOption = SqlOption()
            {
                if (sqlOptions.putIfAbsent(sqlOption.keyString(), sqlOption) != null) {
                    throw SqlUtil.newContextException(getPos(), ParserResource.RESOURCE.duplicateOption(sqlOption.keyString()));
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
 * Parses INSERT/SINK INTO statement.
 */
SqlExtendedInsert SqlExtendedInsert() :
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
    (
        <INSERT>
    |
        <SINK> {
            extendedKeywords.add(SqlExtendedInsert.Keyword.SINK.symbol(getPos()));
        }
    )
    <INTO> { span = span(); }
    SqlInsertKeywords(keywords) {
        keywordList = new SqlNodeList(keywords, span.addAll(keywords).pos());
        extendedKeywordList = new SqlNodeList(extendedKeywords, span.addAll(extendedKeywords).pos());
    }
    table = CompoundIdentifier()
    [
        LOOKAHEAD(2)
        columns = ParenthesizedSimpleIdentifierList()
    ]
    source = QueryOrExpr(ExprContext.ACCEPT_QUERY) {
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
