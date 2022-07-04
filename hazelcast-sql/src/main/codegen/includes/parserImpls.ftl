<#--
// Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
    SqlIdentifier externalName = null;
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
    [
        <EXTERNAL> <NAME> { externalName = SimpleIdentifier(); }
    ]
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
            externalName,
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
    List<SqlNode> columns = new ArrayList<SqlNode>();
}
{
    [
        <LPAREN> {  pos = getPos(); }
        column = MappingColumn()
        {
            columns.add(column);
        }
        (
            <COMMA> column = MappingColumn()
            {
                columns.add(column);
            }
        )*
        <RPAREN>
    ]
    {
        return new SqlNodeList(columns, pos.plus(getPos()));
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
        <EXTERNAL> <NAME> { externalName = SimpleIdentifier(); }
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
    boolean withTimeZone = false;
}
{
    (
        <TIME> { type = QueryDataType.TIME; }
    |
        <DATE> { type = QueryDataType.DATE; }
    |
        <TIMESTAMP>
        withTimeZone = HazelcastTimeZoneOpt()
        {
            if (withTimeZone) {
                type = QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
            } else {
                type = QueryDataType.TIMESTAMP;
            }
        }
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
    (
        <OBJECT> { type = QueryDataType.OBJECT; }
    |
        <JSON> { type = QueryDataType.JSON; }
    )
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
* Parses CREATE INDEX statement.
*/
SqlCreate SqlCreateIndex(Span span, boolean replace) :
{
    SqlParserPos startPos = span.pos();

    SqlIdentifier name;
    SqlIdentifier objectName;
    SqlNodeList attributes;
    SqlIdentifier type = null;
    SqlNodeList sqlOptions = SqlNodeList.EMPTY;
    boolean ifNotExists = false;
}
    {
        <INDEX>
        [
            <IF> <NOT> <EXISTS> { ifNotExists = true; }
        ]
        name = SimpleIdentifier()

        <ON>

        objectName = SimpleIdentifier()
        attributes = IndexAttributes()
        [
            <TYPE>
            type = SimpleIdentifier()
        ]
        [
            <OPTIONS>
            sqlOptions = SqlOptions()
        ]
        {
            return new SqlCreateIndex(
                name,
                objectName,
                attributes,
                type,
                sqlOptions,
                replace,
                ifNotExists,
                startPos.plus(getPos())
        );
    }
}


SqlNodeList IndexAttributes():
{
    SqlParserPos pos = getPos();

    SqlIdentifier attributeName;
    List<SqlNode> attributes = new ArrayList<SqlNode>();
}
{
    [
        <LPAREN> {  pos = getPos(); }
        attributeName = SimpleIdentifier()
        {
            attributes.add(attributeName);
        }
        (
            <COMMA> attributeName = SimpleIdentifier()
            {
                attributes.add(attributeName);
            }
        )*
        <RPAREN>
    ]
    {
        return new SqlNodeList(attributes, pos.plus(getPos()));
    }
}

/**
 * Parses DROP INDEX statement.
 */
SqlDrop SqlDropIndex(Span span, boolean required) :
{
    SqlParserPos pos = span.pos();

    SqlIdentifier name;
    SqlIdentifier objectName;
    boolean ifExists = false;
}
{
    <INDEX>
    [
        <IF> <EXISTS> { ifExists = true; }
    ]
    name = SimpleIdentifier()

    <ON>
    objectName = SimpleIdentifier()
    {
        return new SqlDropIndex(name, objectName, ifExists, pos.plus(getPos()));
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
    SqlExtendedInsert sqlInsert;
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
            replace,
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
 * Parses CREATE VIEW statement.
 */
SqlCreate SqlCreateView(Span span, boolean replace) :
{
    SqlParserPos startPos = span.pos();
    boolean ifNotExists = false;
    SqlIdentifier name;
    SqlNode query;
}
{
    <VIEW>
    [
        <IF> <NOT> <EXISTS> { ifNotExists = true; }
    ]
    name = CompoundIdentifier()

    <AS>

    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new com.hazelcast.jet.sql.impl.parse.SqlCreateView(
            startPos,
            replace,
            ifNotExists,
            name,
            query
        );
    }
}

/**
 * Parses DROP VIEW statement.
 */
SqlDrop SqlDropView(Span span, boolean replace) :
{
    SqlParserPos pos = span.pos();

    SqlIdentifier name;
    boolean ifExists = false;
}
{
    <VIEW>
    [
        <IF> <EXISTS> { ifExists = true; }
    ]
    name = SimpleIdentifier()
    {
        return new SqlDropView(name, ifExists, pos.plus(getPos()));
    }
}

/**
 * Parses OPTIONS.
 */
SqlNodeList SqlOptions():
{
    Span span;

    SqlOption sqlOption;
    List<SqlNode> sqlOptions = new ArrayList<SqlNode>();
}
{
    <LPAREN> { span = span(); }
    [
        sqlOption = SqlOption()
        {
            sqlOptions.add(sqlOption);
        }
        (
            <COMMA> sqlOption = SqlOption()
            {
                sqlOptions.add(sqlOption);
            }
        )*
    ]
    <RPAREN>
    {
        return new SqlNodeList(sqlOptions, span.end(this));
    }
}

SqlOption SqlOption() :
{
    Span span;
    SqlNode key, value;
}
{
    key = StringLiteral() { span = span(); }
    <EQ>
    value = StringLiteral()
    {
        return new SqlOption(key, value, span.end(this));
    }
}

/**
* Parses SHOW statements.
*/
SqlShowStatement SqlShowStatement() :
{
    ShowStatementTarget target;
}
{
    <SHOW>
    (
        [ <EXTERNAL> ] <MAPPINGS> { target = ShowStatementTarget.MAPPINGS; }
    |
        <VIEWS> { target = ShowStatementTarget.VIEWS; }
    |
        <JOBS> { target = ShowStatementTarget.JOBS; }
    )
    {
        return new SqlShowStatement(getPos(), target);
    }
}

/**
 * Parses an EXPLAIN statement.
 */
SqlNode SqlExplainStatement() :
{
    SqlNode stmt;
}
{
    <EXPLAIN>
    [
        LOOKAHEAD(2)
        <PLAN> <FOR>
    ]
    stmt = ExtendedSqlQueryOrDml() {
        return new SqlExplainStatement(getPos(), stmt);
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

/** Parses a query (SELECT or VALUES)
 * or DML statement (extended INSERT, UPDATE, DELETE). */
SqlNode ExtendedSqlQueryOrDml() :
{
    SqlNode stmt;
}
{
    (
        LOOKAHEAD(2)
        stmt = SqlExtendedInsert()
    |
        stmt = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    |
        stmt = SqlDelete()
    |
        stmt = SqlUpdate()
    ) { return stmt; }
}

/**
 * Hazelcast specific date-time types parsing.
 */
SqlTypeNameSpec HazelcastDateTimeTypeName() :
{
    SqlTypeName typeName;
    boolean withTimeZone = false;
}
{
    <DATE> {
        typeName = SqlTypeName.DATE;
        return new SqlBasicTypeNameSpec(typeName, getPos());
    }
|
    LOOKAHEAD(2)
    <TIME> {
        typeName = SqlTypeName.TIME;
        return new SqlBasicTypeNameSpec(typeName, -1, getPos());
    }
|
    <TIMESTAMP>
    withTimeZone = HazelcastTimeZoneOpt()
    {
        if (withTimeZone) {
            typeName = SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
        } else {
            typeName = SqlTypeName.TIMESTAMP;
        }
        return new SqlBasicTypeNameSpec(typeName, -1, getPos());
    }
}

boolean HazelcastTimeZoneOpt() :
{
}
{
    LOOKAHEAD(3)
    <WITHOUT> <TIME> <ZONE> { return false; }
|
    <WITH> <TIME> <ZONE> { return true; }
|
    { return false; }
}
