# SQL Expressions

## Overview

Expressions are one of the core elements of every SQL implementation.
Expressions can participate in projections (`SELECT a + b FROM t`) and
filters (`SELECT * FROM t WHERE a + b < 10`). Expressions may be roughly
subdivided into 4 general categories:

- Terminal expressions not referring to other expressions: column
  references, literals, dynamic parameters and type expressions (i.e.
  the type argument of the `CAST` expression).
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
logic to align it with our requirements: the narrowest integer type is
assigned to integer literals and to `CAST` operators involving integer
types.

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

- `SqlValidator.deriveType` may customize the behavior globally, it's
  the main source of truth for node types and invoked every time when a
  node type is needed. Derived types are cached by the validator to
  avoid constant re-derivation. Usually, if a node in question is an
  operator the validator just delegates the type derivation to it using
  `SqlOperator.deriveType`. Which in turn, usually, delegates back to
  the validator to derive the types of its operands and then calls
  `SqlOperator.inferReturnType`, which may delegate to
  `SqlReturnTypeInference` strategy associated with the operator.

- Each `SqlOperator` implementation may customize its operand type
  inference strategy (see `SqlOperandTypeInference` and
  `SqlOperator.getOperandTypeInference`) and its return type inference
  strategy (see `SqlReturnTypeInference`,
  `SqlOperator.getReturnTypeInference` and
  `SqlOperator.inferReturnType`). Also, operand type validation strategy
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
  narrowest integer type possible (`TINYINT`, `SMALLINT`, `INTEGER` or
  `BIGINT`); numeric literals containing decimal point (`1.1`, `4.2`,
  etc.) receive `DECIMAL` type; scientific notation numeric literals
  (`1e1`, `4.2e2`, etc.) receive `DOUBLE` type; string literals
  (`'foo'`) receive `VARCHAR` type.

- Parameters: parameter types are inferred from the context: `1.0 + ?`,
  the parameter would receive `DECIMAL` type since the type of the
  left-hand side literal is `DECIMAL`; `1 + ?`, despite the fact that
  the literal type is `TINYINT` the parameter would receive `BIGINT`
  type because `TINYINT` is too restrictive; `sin(?)`, the parameter
  would receive `DOUBLE` type since it's the only type accepted by `sin`
  function. In certain cases (`SELECT ?`, for instance) it's impossible
  to infer a concrete type, more on that below.
  
In general, the validation process begins with a call to
`SqlValidator.validate`, which starts a recursive validation of the
passed node and its children. Typically, during the validation, types
should be _derived_ for every node starting at the root, that usually
requires knowing types of all child nodes. Types of certain nodes
(dynamic parameters, for instance) might be unknown, so as a first step
every node tries to _infer_ types for its child nodes of unknown types.
For operators this process can be customized using
`SqlOperandTypeInference` strategy associated with an operator.

If a node type can't be inferred, the validation fails. Currently, that
happens if an operator acts only on dynamic parameters and/or `NULL`s.
This might change in the future, for instance we may assign some default
type in such cases. Consider `? + ?`, `+` operator can be applied to
numeric and temporal types, we may choose to assign `DECIMAL` type for
the parameters in this specific case. Alternatively, we might take into
account the actual types of the passed parameter arguments and construct
separate query plans based on that information, that way we would not
need any one-size-fits-all defaults. `NULL` as a type is not a
first-class citizen in Calcite: basically, every `NULL` literal
participating in some operator or function must have a concrete type
assigned, therefore `NULL + NULL` is affected by the same type inference
problem, which might be solved by making `NULL` type a first-class
citizen or by choosing some defaults.

After all unknown types are inferred and assigned to nodes, a type for
every tree node can be derived from the node itself potentially
consulting its child nodes for their types. Most operators have an
additional round of type refinement called type coercion. For instance,
both sides of the binary comparison operators are coerced to the same
type respecting type precedence and conversion rules defined in [Type
System design document](01-type-system.md). Another example is the
binary arithmetic operators coercing their operands to a common type.

Each operator validates that it has a proper number of operands of
proper types. The validation process can be customized by overriding
`SqlOperator.checkOperandTypes` and `SqlOperator.getOperandCountRange`,
or by providing a custom `SqlOperandTypeChecker` strategy for an
operator. For instance, `HazelcastSqlCastFunction` overrides
`checkOperandTypes` to make sure the casting behaviour is exactly the
same as defined by Type System conversion rules. Calcite provides a
collection of `SqlOperandTypeChecker` strategies in `OperandTypes`
class. For every operator we provide our own type checking strategy.

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
corresponding `Expression` instance.

The final result of the translation stage is an `Expression` tree ready
for runtime evaluation. No further changes are expected to the tree
after this point.

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
expression should be evaluated. The expression doesn't necessary need to
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
its runtime counterpart based on `Expression` interface.
If the pre-existing implementation requires some modifications of its
behavior, try to modify it using parametrization or inheritance, resort
to copying if that's impossible. If there is no pre-existing implementation
provided by Calcite, create it from scratch. 
Add the translation support to `RexToExpressionVisitor` and make sure the
corresponding operator or function is listed as allowed in
`UnsupportedOperationVisitor`. Add tests based on `ExpressionTestSupport`.
