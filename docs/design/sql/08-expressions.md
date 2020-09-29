# SQL Expressions

## Overview

Expressions is one of the core elements of every SQL implementation.
Expressions can participate in projections (`SELECT a + b FROM t`) and
filters (`SELECT * FROM t WHERE a + b < 10`). Expressions may be roughly
subdivided into 4 general categories:

- Terminal expressions not referring to other expressions: column,
  literal, parameter and type expressions.
- Operators, like numeric arithmetic operators: `+`, `-`, etc.
- Predicates, like comparison predicates: `<`, `>`, etc.
- Functions, like `ABS`, `SIN`, etc.

## Implementation Details

We use facilities provided by Calcite to support expression parsing and
validation. Every SQL expression goes through the following stages:

- Parsing
- Validation
- Conversion
- Translation
- Evaluation

### Parsing Stage

The goal of the parsing stage is to obtain an AST expression
representation from the textual expression representation for
syntactically valid constructs and report errors for invalid ones. For
instance, `a + b` is syntactically valid and `a +` is invalid.

In Calcite, AST nodes are represented as subclasses of `SqlNode`. Column
and type references are represented by `SqlIdentifier`. Literals are
represented by `SqlLiteral` and its subclasses. Parameters are
represented by `SqlDynamicParam`. Most other, if not all, expressions
are subclasses of `SqlCall`.

### Validation Stage

The goal of the validation stage is to make sure that the AST
expressions produced by the previous parsing stage are semantically
valid. For instance, `1 + 1` is semantically valid while `1 + true` is
not.

In Calcite, the validation logic is encapsulated and orchestrated by
implementations of `SqlValidator`. Specifically, `SqlValidatorImpl`
extended by `HazelcastSqlValidator` is used to customize the validation
logic to align it with our requirements.

As a first step of the validation process, `SqlValidatorImpl` may apply
some transformations to the passed AST which should not affect the
semantics of the AST. Such transformations are called "unconditional
rewrites" (see `SqlValidatorImpl.performUnconditionalRewrites`). Every
`SqlCall` node has an associated `SqlOperator` returned by its
`getOperator` method. `SqlOperator` implementations may customize
various aspects of a certain operator including the rewriting behavior
itself (see `SqlOperator.rewriteCall`).

Normally, Calcite's parser inspects the hardcoded `SqlStdOperatorTable`
implementation of `SqlOperatorTable` to resolve a certain operator, so
it's hard to customize the pre-existing operators. To make the
customization possible, operators from `SqlStdOperatorTable` are patched
to the ones from `HazelcastSqlOperatorTable` during the AST rewriting
process (see `HazelcastSqlValidator.performUnconditionalRewrites` and
`HazelcastOperatorTableVisitor`).

Usually, semantic validation requires assigning types to every
expression and its operand expressions, if any. In Calcite, that type
assignment behavior may be customized in several ways:

- `SqlValidator.deriveType` may customize the behavior globally.

- Each `SqlOperator` implementation may customize its operand type
  inference strategy (see `SqlOperandTypeInference` and
  `SqlOperator.getOperandTypeInference`) and its return type inference
  strategy (see `SqlReturnTypeInference`,
  `SqlOperator.getReturnTypeInference` and
  `SqlOperator.inferReturnType`). The first one is used only if some of
  the operand types are unknown. Also, operand type validation strategy
  can be customized using an associated `SqlOperandTypeChecker` (see
  `SqlOperator.getOperandTypeChecker`).

- Additionally, some expressions require their operands to be coerced to
  a certain type. For instance, `=` operator requires both of its sides
  to be of the same type. In Calcite, such coercion services are
  provided by `TypeCoercion` implementations. Specifically,
  `TypeCoercionImpl` extended by `HazelcastTypeCoercion` is used to
  customize the coercion logic.

The following general rules apply when assigning types:

- Literals: `TRUE` and `FALSE` literals receive `BOOLEAN` type; numeric
  literals containing no decimal point (`1`, `42`, etc.) receive the
  smallest integer type possible (`TINYINT`, `SMALLINT`, `INTEGER` or
  `BIGINT`); numeric literals containing decimal point (`1.1`, `4.2`,
  etc.) receive `DECIMAL` type; scientific notation numeric literals
  (`1e1`, `4.2e2`, etc.) receive `DOUBLE` type; string literals
  (`'foo'`) receive `VARCHAR` type.

- Parameters: parameter types are inferred from the context: `1.0 + ?`,
  the parameter would receive `DOUBLE` type.

As an end result, for semantically valid ASTs, the validation process
produces a potentially transformed AST where every node has a known
type. For semantically invalid ASTs, an error is reported.

### Conversion Stage

The goal of the conversion stage it to convert the syntactically and
semantically valid AST (`SqlNode`) received from the previous stages to
a representation suitable for the relational optimization. In Calcite,
relational nodes are represented by subclasses of `RelNode` while
expression nodes referenced from relational ones are represented by
subclasses of `RexNode`.

During conversion Calcite also applies various simplifications and
optimizations to expressions (see `SqlToRelConverter` and
`HazelcastSqlToRelConverter`). After the conversion all references to
the original textual SQL query representation are irreversibly lost. In
other words, it's impossible to recover a `SqlNode` from a certain
`RexNode` or `RelNode`.

The resulting expressions may also undergo various transformations as
dictated by relational optimizations applied in other parts of the SQL
engine.

### Translation Stage

The goal of the translation phase is to translate the `RexNode`
representation received from the previous stage to a representation
suitable for the runtime evaluation of expressions.

The translation is performed by `RexToExpressionVisitor` with the help
of `RexToExpression`. Every instance of `RexNode` is translated into a
corresponding `Expression` instance:

The final result of the translation stage is an `Expression` tree ready
for runtime evaluation. No further changes expected to the tree after
this point.

### Evaluation Stage

Each `Expression` implements `eval` method to support runtime
evaluation:

```java
public interface Expression<T> ... {
    ...

    T eval(Row row, ExpressionEvalContext context);

    ...
}
```

`eval` is provided with a row and an evaluation context on which the
expression should be evaluated. The Expression doesn't necessary need to
access the row or the context during the evaluation, for instance
`ConstantExpression` just returns literal values. Currently,
`ExpressionEvalContext` provides access only to the actual dynamic
parameter values passed to the query. In future it might be extended
with some additional information.

## Implementing Expressions

First of all, check for the pre-existing operator or function
implementation that might be provided by Calcite. Usually, references to
such pre-existing implementations are listed in `SqlStdOperatorTable`
and the implementations themselves are located at
`org.apache.calcite.sql.fun` package.

If the pre-existing implementation satisfies the requirements, create
its runtime counterpart based on `Expression` interface. Add the
translation support to `RexToExpressionVisitor` and make sure the
corresponding operator or function is listed as allowed in
`UnsupportedOperationVisitor`. Add tests based on `ExpressionTestBase`
and `SqlExpressionIntegrationTestSupport`.

If the pre-existing implementation requires some modifications of its
behavior, try to modify it using parametrization or inheritance, resort
to copying if that's impossible. Add a reference to the new modified
implementation in `HazelcastSqlOperatorTable` to make it visible to
`HazelcastOperatorTableVisitor`.

If there is no pre-existing implementation provided by Calcite, create
it from scratch. Add a reference to it in `HazelcastSqlOperatorTable`,
list it as allowed in `UnsupportedOperationVisitor`, create runtime
counterpart based on `Expression`, add translation support to
`RexToExpressionVisitor`, add tests based on `ExpressionTestBase`and
`SqlExpressionIntegrationTestSupport`.
