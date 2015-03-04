

# Preface

Welcome to the Hazelcast Reference Manual. This manual includes concepts, instructions and samples to guide you on how to use Hazelcast and build Hazelcast applications.

As the reader of this manual, you must be familiar with the Java programming language and you should have installed your preferred IDE.

##### Product Naming

Throughout this manual:

- **Hazelcast** refers to the open source edition of Hazelcast in-memory data grid middleware. It is also the name of the company providing the Hazelcast product.
- **Hazelcast Enterprise** refers to the commercial edition of Hazelcast.

##### Licensing

Hazelcast is free provided under the Apache 2 license. Hazelcast Enterprise is commercially licensed by Hazelcast, Inc.

For more detailed information on licensing, please see the [License Questions appendix](#license-questions).

##### Trademarks

Hazelcast is a registered trademark of Hazelcast, Inc. All other trademarks in this manual are held by their respective owners. 


##### Customer Support

Support for Hazelcast is provided via [GitHub](https://github.com/hazelcast/hazelcast/issues), [Mail Group](https://groups.google.com/forum/#!forum/hazelcast) and [StackOverflow](http://www.stackoverflow.com). 

For information on support for Hazelcast Enterprise, please see [hazelcast.com/pricing](http://hazelcast.com/pricing/).

##### Contributing to Hazelcast

You can contribute to the Hazelcast code, report a bug or request an enhancement. Please see the following resources.

- [Developing with Git](https://hazelcast.atlassian.net/wiki/display/COM/Developing+with+Git): Document that explains the branch mechanism of Hazelcast and how to request changes.
- [Hazelcast Contributor Agreement form](https://hazelcast.atlassian.net/wiki/display/COM/Hazelcast+Contributor+Agreement): Form that each contributing developer needs to fill and send back to Hazelcast.
- [Hazelcast on GitHub](https://github.com/hazelcast/hazelcast): Hazelcast repository where the code is developed, issues and pull requests are managed.


##### Typographical Conventions

Below table shows the conventions used in this manual.

|Convention|Description|
|:-|:-|
|**bold font**| - Indicates part of a sentence that require the reader's specific attention. <br> - Also indicates property/parameter values.|
|*italic font*|- When italicized words are enclosed with "<" and ">", indicates a variable in command or code syntax that you must replace, e.g. `hazelcast-<`*version*`>.jar`. <br> - Note and Related Information text is in italics.|
|`monospace`|- Indicates files, folders, class and library names, code snippets, and inline code words in a sentence.|
|***RELATED INFORMATION***|- Indicates a resource that is relevant to the topic, usually with a link or cross-reference.|
|![image](images/NoteSmall.jpg) ***NOTE***| Indicates information that is of special interest or importance, e.g. an additional action required only in certain circumstances.|
|element & attribute|Mostly used in the context of declarative configuration, i.e. configuration performed by the Hazelcast XML file.  Element refers to an XML tag used to configure a Hazelcast feature. Attribute is a parameter owned by an element, contributing into the declaration of that element's configuration. Please see the following example.<br></br>`<port port-count="100">5701</port>`<br></br> In this example, `port-count` is an **attribute** of the `port` **element**.

<br></br>







