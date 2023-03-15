# CodeQL workflow in Hazelcast

The [PR #21663](https://github.com/hazelcast/hazelcast/pull/21663) introduced
a CodeQL code scanning to `hazelcast/hazelcast` GitHub repository.

Due to issues in some Java queries included in CodeQL, a custom configuration is also
provided in the `.github/codeql` directory (where this `README.md` file is also located).

The [`codeql-config.yml`](codeql-config.yml) file disables default Java queries and references
a custom query definition file [`java-custom-queries.qls`](java-custom-queries.qls).

The `java-custom-queries.qls` is based on default set of Java queries but it excludes
the ones which proved problematic (hanging) in `hazelcast` repository.

## Reference

Check the problem types checked by the CodeQL workflow in the documentation:

* https://codeql.github.com/codeql-query-help/java/
* https://codeql.github.com/codeql-query-help/java-cwe/

## ToDo

We should get rid of the custom CodeQL configuration and use the default queries once
the "hanging issues" are resolved on the CodeQL side.
