#Protocol Messages

## Compound Data Types Used In The Protocol Specification
Some common compound data structures used in the protocol message specification are defined in this section.

### Array
In the protocol specification, an array of a data type is frequently used. An array of a data type with n entries 
is encoded as shown below:

|Field|Type|Nullable|Description|
|-----|----|---------|----------|
|Length|int32|No|The length of the array. In this example, it is a value of n.|
|Entry 1|provided data type|No|First entry of the array|
|Entry 2|provided data type|No|Second entry of the array|
|...|...|...|...|
|...|...|...|...|
|Entry n|provided data type|No|n'th entry of the array|

### Address Data Type
| Field| Type| Nullable| Description|
|------|-----|---------|------------|
|Host|string|No|The name or the IP address of the server member|
|Port|int32|No|The port number used for this address|

### Cache Event Data Type
| Field| Type| Nullable| Description|
|------|-----|---------|------------|
|Cache Event type|int32|No|The type of the event. Possible values and their meanings are:<br> CREATED(1):An event type indicating that the cache entry was created. <br>UPDATED(2): An event type indicating that the cache entry was updated, i.e. a previous mapping existed. <br>REMOVED(3): An event type indicating that the cache entry was removed. <br>EXPIRED(4): An event type indicating that the cache entry has expired.<br>EVICTED(5): An event type indicating that the cache entry has evicted. <br>INVALIDATED(6): An event type indicating that the cache entry has invalidated for near cache invalidation. <br>COMPLETED(7): An event type indicating that the cache operation has completed. <br>EXPIRATION_TIME_UPDATED(8): An event type indicationg that the expiration time of cache record has been updated
|Name|string|No|Name of the cache|
|Key|byte-array|Yes|Key of the cache data|
|Value|byte-array|Yes|Value of the cache data|
|Old Value|byte-array|Yes|Old value of the cache data if exists|
|Value|byte-array|Yes|Value of the cache data|
|isOldValueAvailable|boolean|No|True if old value exist|

### Distributed Object Info Data Type
| Field| Type| Nullable| Description|
|------|-----|---------|------------|
|Service Name|string|No|Name of the service for the distributed object. <br>E.g. this is "hz:impl:cacheService" for Cache object|
|Name|string|No|Name of the object|

### Entry View Data Type
| Field| Type| Nullable| Description|
|------|-----|---------|------------|
|Key|byte-array|No|Key of the entry|
|Value|byte-array|No|Value of the entry|
|Cost|int64|No|Cost of the entry|
|Creation Time|int64|No|Time when the entry is created|
|Expiration Time|int64|No|Time when the entry will expiry|
|Hits|int64|No|Number of hits|
|Last Access Time|int64|No|Time when entry is last accessed|
|Last Stored Time|int64|No|Time when entry is last stored|
|Last Update Time|int64|No|Time when entry is last updated|
|Version|int64|No|Version of the entry|
|Eviction Criteria Number|int64|No|The number of the eviction criteria applied|
|ttl|int64|No|Time to live for the entry|

### Job Partition State Data Type
| Field| Type| Nullable| Description|
|------|-----|---------|------------|
|Owner Address|Address|No|The address of the partition owner|
|State value|string|No|Value of the partition state. Possible values are:<br>"WAITING": Partition waits for being calculated. <br>"MAPPING": Partition is in mapping phase. <br>"REDUCING": Partition is in reducing phase (mapping may still not finished when this state is reached since there is a chunked based operation underlying). <br>"PROCESSED": Partition is fully processed <br>"CANCELLED": Partition calculation cancelled due to an internal exception

### Member Data Type
| Field| Type| Nullable| Description|
|------|-----|---------|------------|
|Address|Address|No|Address of the member server|
|Uuid|string|No|Unique user id of the member server|
|attribute 1 name|string|No|Name of the attribute 1|
|attribute 1 value|string|No|Value of the attribute 1|
|attribute 2 name|string|No|Name of the attribute 2|
|attribute 2 value|string|No|Value of the attribute 2|
|...|...|...|...|
|...|...|...|...|
|attribute n name|string|No|Name of the attribute n|
|attribute n value|string|No|Value of the attribute n|

<br>n is the number of attributes for the server member.

### Query Cache Event Data Type
| Field| Type| Nullable| Description|
|------|-----|---------|------------|
|Event Data|int64|No|The data for the cache event|
|Key|byte-array|Yes|The key for the event|
|New Value|byte-array|Yes|The new value for the event|
|Event type|int32|No|The type of the event|
|Partition Id|int32|No|The partition id for the event key|

### Transaction Id Data Type
| Field| Type| Nullable| Description|
|------|-----|---------|------------|
|Format Id|int32|No|The id of the transaction format|
|Global Transaction Id|byte-array|No|The global id for the transaction|
|Branch Qualifier|byte-array|No|The qualifier for the branch|

### Stack Trace Data type
| Field| Type| Nullable| Description|
|------|-----|---------|------------|
|Declaring Class|string|No|The name of the class|
|Method Name|string|No|The name of the method|
|File Name|string|Yes|The name of the class|
|Line Number|int32|No|The line number in the source code file|

## Error Message
Response Message Type Id: 109

| Field| Type| Nullable| Description|
|------|-----|---------|------------|
|Error Code|int32|No|The unique code identifying the error|
|Class Name|string|No|The class name which caused the error at the server side|
|Message|string|Yes|The brief description of the error|
|Stack Trace|array of stack-trace|No|The stack trace at the server side when the error occured|
|Cause Error Code|int32|No|The error code for the actual cause of the error. If no cause exists, it is set to -1|
|Cause Class Name|string|Yes|The name of the class that actually cause the error|

The following error codes are defined in the system:

| Error Name| Error Code| Description|
|-----------|-----------|------------|
|UNDEFINED|0|The error is not in the defined list of protocol errors.|
|ARRAY_INDEX_OUT_OF_BOUNDS|1|Thrown to indicate that an array has been accessed with an illegal index. The index is either negative or greater than or equal to the size of the array.|
|ARRAY_STORE|2| Thrown to indicate that an attempt has been made to store the wrong type of object into an array of objects. For example, the following code generates an ArrayStoreException:<br>Object x[] = new String[3];<br>x[0] = new Integer(0);|
|AUTHENTICATION|3|The authentication failed.|
|CACHE|4||
|CACHE_LOADER|5||
|CACHE_NOT_EXISTS|6|This exception class is thrown while creating com.hazelcast.cache.impl.CacheRecordStore instances but the cache config does not exist on the node to create the instance on. This can happen in either of two cases:<br>the cache's config is not yet distributed to the node, or <br>the cache has been already destroyed.<br> For the first option, the caller can decide to just retry the operation a couple of times since distribution is executed in a asynchronous way.
|
|CACHE_WRITER|7||
|CALLER_NOT_MEMBER|8|A Retryable Hazelcast Exception that indicates that an operation was sent by a machine which isn't member in the cluster when the operation is executed.
|
|CANCELLATION|9|Exception indicating that the result of a value-producing task, such as a FutureTask, cannot be retrieved because the task was cancelled.|
|CLASS_CAST|10|The class conversion (cast) failed.|
|CLASS_NOT_FOUND|11|The class does not exists in the loaded jars at the server member.|
|CONCURRENT_MODIFICATION|12|You are trying to modify a resource concurrently which is not allowed.|
|CONFIG_MISMATCH|13|Thrown when 2 nodes want to join, but their configuration doesn't match.|
|CONFIGURATION|14|Thrown when something is wrong with the server or client configuration.|
|DISTRIBUTED_OBJECT_DESTROYED|15|The distributed object that you are trying to access is destroyed and does not exist.|
|DUPLICATE_INSTANCE_NAME|16|An instance with the same name already exists in the system.|
|EOF|17|End of file is reached (May be for a file or a socket)|
|ENTRY_PROCESSOR|18||
|EXECUTION|19|Thrown when attempting to retrieve the result of a task that aborted by throwing an exception.|
|HAZELCAST|20|General internal error of Hazelcast.|
|HAZELCAST_INSTANCE_NOT_ACTIVE|21|The Hazelcast server instance is not active, the server is possibly initialising.|
|HAZELCAST_OVERLOAD|22|Thrown when the system won't handle more load due to an overload. This exception is thrown when backpressure is enabled.|
|HAZELCAST_SERIALIZATION|23|Error during serialisation/de-serialisation of data.|
|IO|24|An IO error occured.|
|ILLEGAL_ARGUMENT|25||
|ILLEGAL_ACCESS_EXCEPTION|26||
|ILLEGAL_ACCESS_ERROR|27||
|ILLEGAL_MONITOR_STATE|28|When an operation on a distributed object is being attempted by a thread which did not initially own the lock on the object.|
|ILLEGAL_STATE|29||
|ILLEGAL_THREAD_STATE|30|Thrown to indicate that a thread is not in an appropriate state for the requested operation.|
|INDEX_OUT_OF_BOUNDS|31|Thrown to indicate that an index of some sort (such as to a list) is out of range.|
|INTERRUPTED|32||
|INVALID_ADDRESS|33|Thrown when given address is not valid.|
|INVALID_CONFIGURATION|34|An InvalidConfigurationException is thrown when there is an Invalid Configuration. Invalid Configuration can be a wrong Xml Config or logical config errors that are found at real time.|
|MEMBER_LEFT|35|Thrown when a member left during an invocation or execution.|
|NEGATIVE_ARRAY_SIZE|36|The provided size of the array can not be negative but a negative number is provided.|
|NO_SUCH_ELEMENT|37|The requested element does not exist in the distributed object.|
|NOT_SERIALIZABLE|38|The object could not be serialised|
|NULL_POINTER|39|The server faced a null pointer exception during the operation.|
|OPERATION_TIMEOUT|40| An unchecked version of java.util.concurrent.TimeoutException. <p>Some of the Hazelcast operations may throw an <tt>OperationTimeoutException</tt>. Hazelcast uses OperationTimeoutException to pass TimeoutException up through interfaces that don't have TimeoutException in their signatures.</p>
|
|PARTITION_MIGRATING|41|Thrown when an operation is executed on a partition, but that partition is currently being moved around.|
|QUERY|42|Error during query.|
|QUERY_RESULT_SIZE_EXCEEDED|43|Thrown when a query exceeds a configurable result size limit.|
|QUORUM|44|An exception thrown when the cluster size is below the defined threshold.|
|REACHED_MAX_SIZE|45|Exception thrown when a write-behind MapStore rejects to accept a new element.|
|REJECTED_EXECUTION|46|Exception thrown by an Executor when a task cannot be accepted for execution.|
|REMOTE_MAP_REDUCE|47|This is used for failed remote operations. This can happen if the get result operation fails to retrieve values for some reason.|
|RESPONSE_ALREADY_SENT|48|There is some kind of system error causing a response to be send multiple times for some operation.|
|RETRYABLE_HAZELCAST|49|The operation request can be retried.|
|RETRYABLE_IO|50|Indicates that an operation can be retried. E.g. if map.get is send to a partition that is currently migrating, a subclass of this exception is thrown, so the caller can deal with it (e.g. sending the request to the new partition owner).|
|RUNTIME|51||
|SECURITY|52|There is a security violation.|
|SOCKET|53|There is an error in the underlying TCP protocol|
|STALE_SEQUENCE|54|Thrown when accessing an item in the Ringbuffer using a sequence that is smaller than the current head sequence. This means that the and old item is read, but it isn't available anymore in the ringbuffer.|
|TARGET_DISCONNECTED|55|Indicates that an operation is about to be sent to a non existing machine.|
|TARGET_NOT_MEMBER|56|Indicates operation is sent to a machine that isn't member of the cluster.|
|TIMEOUT|57||
|TOPIC_OVERLOAD|58|Thrown when a publisher wants to write to a topic, but there is not sufficient storage to deal with the event. This exception is only thrown in combination with the reliable topic.|
|TOPOLOGY_CHANGED|59|Thrown when a topology change happens during the execution of a map reduce job and the com.hazelcast.mapreduce.TopologyChangedStrategy is set to com.hazelcast.mapreduce.TopologyChangedStrategy#CANCEL_RUNNING_OPERATION}.|
|TRANSACTION|60|Thrown when something goes wrong while dealing with transactions and transactional data-structures.|
|TRANSACTION_NOT_ACTIVE|61|Thrown when an a transactional operation is executed without an active transaction.|
|TRANSACTION_TIMED_OUT|62|Thrown when a transaction has timed out.|
|URI_SYNTAX|63||
|UTF_DATA_FORMAT|64||
|UNSUPPORTED_OPERATION|65|The message type id for the operation request is not a recognised id.|
|WRONG_TARGET|66|An operation is executed on the wrong machine.|
|XA|67|An error occured during an XA operation.|
|ACCESS_CONTROL|68|Indicates that a requested access to a system resource is denied.|
|LOGIN|69||
|UNSUPPORTED_CALLBACK|70|Signals that a CallbackHandler does not recognize a particular Callback.|

<#list model?keys as key>
<#assign map=model?values[key_index]?values/>
<#if map?has_content>

<#if key == "com.hazelcast.client.impl.protocol.template.ClientMessageTemplate">
##General Protocol Operations (No Specific Object)
<#else>
##${util.getDistributedObjectName(key)} Object
</#if>

<#list map as cm>
###"${cm.name?cap_first}" Operation

Request Message Type Id:${cm.id}, Retryable:<#if cm.retryable == 1 >Yes<#else>No</#if>
${util.getOperationDescription(cm.comment)}

    <#if cm.requestParams?has_content>

| Name| Type| Nullable| Description|
|-----|-----|---------|------------|
        <#list cm.requestParams as param>
|${param.name}| ${convertTypeToDocumentType(param.type)}| <#if param.nullable >Yes<#else>No</#if>|${util.getDescription(param.name, cm.comment)}|
        </#list>
    <#else>
Header only request message, no message body exist.
    </#if>

Response Message Type Id:${cm.response}

${util.getReturnDescription(cm.comment)}

    <#if cm.responseParams?has_content>

| Name| Type| Nullable|
|-------|------------|----------|
        <#list cm.responseParams as param>
|${param.name}| ${convertTypeToDocumentType(param.type)}| <#if param.nullable >Yes<#else>No</#if>|
        </#list>

    <#else>

Header only response message, no message body exist.
    </#if>

    <#if cm.events?has_content>

<#list cm.events as event >
<br>"${event.name}" Event Message

Message Type Id:${event.type}

    <#if event.eventParams?has_content>

| Name| Type| Nullable| Description|
|-------|------------|----------|------------|
        <#list event.eventParams as param>
|${param.name}| ${convertTypeToDocumentType(param.type)}| <#if param.nullable >Yes<#else>No</#if>|${param.description}|
        </#list>
    <#else>

Header only event message, no message body exist.
    </#if>

</#list>

    </#if>

</#list>

<#if key == "com.hazelcast.client.impl.protocol.template.EnterpriseMapCodecTemplate">
<b>Note:</b> All operation defined for the Map Object can also be executed against the EnterpriseMap Object.
</#if>

</#if>
</#list>

<#function convertTypeToDocumentType javaType>
    <#switch javaType?trim>
        <#case "int">
            <#return "int32">
        <#case "integer">
            <#return "int32">
        <#case "short">
            <#return "int16">
        <#case "boolean">
            <#return "boolean">
        <#case "byte">
            <#return "uint8">
        <#case "long">
            <#return "int64">
        <#case "char">
            <#return "int8">
        <#case util.DATA_FULL_NAME>
            <#return "byte-array">
        <#case "java.lang.String">
            <#return "string">
        <#case "boolean">
            <#return "boolean">
        <#case "java.util.List<" + util.DATA_FULL_NAME + ">">
            <#return "array of byte-array">
        <#case "java.util.Set<" + util.DATA_FULL_NAME + ">">
            <#return "array of byte-array">
        <#case "java.util.Set<com.hazelcast.core.Member>">
            <#return "array of Member">
        <#case "java.util.Set<com.hazelcast.client.impl.client.DistributedObjectInfo>">
            <#return "array of Distributed Object Info">
        <#case "java.util.Map<com.hazelcast.nio.Address,java.util.Set<java.lang.Integer>>">
            <#return "array of Address-Partition Id pair">
        <#case "java.util.Collection<" + util.DATA_FULL_NAME + ">">
            <#return "array of byte-array">
        <#case "java.util.Map<" + util.DATA_FULL_NAME + "," + util.DATA_FULL_NAME + ">">
            <#return "array of key-value byte array pair">
        <#case "java.util.Set<java.util.Map.Entry<"+ util.DATA_FULL_NAME + "," + util.DATA_FULL_NAME + ">>">
            <#return "array of key-value byte array pair">
        <#case "com.hazelcast.map.impl.SimpleEntryView<" + util.DATA_FULL_NAME +"," + util.DATA_FULL_NAME +">">
            <#return "array of Entry View">
        <#case "com.hazelcast.nio.Address">
            <#return "Address">
        <#case "com.hazelcast.core.Member">
            <#return "Member">
        <#case "javax.transaction.xa.Xid">
            <#return "Transaction Id">
        <#case "com.hazelcast.cluster.client.MemberAttributeChange">
            <#return "Member Attribute Change">
        <#case "com.hazelcast.map.impl.querycache.event.QueryCacheEventData">
            <#return "Query Cache Event Data">
        <#case "java.util.List<com.hazelcast.mapreduce.JobPartitionState>">
            <#return "array of Job Partition State">
        <#case "java.util.Set<com.hazelcast.cache.impl.CacheEventData>">
            <#return "array of Cache Event Data">
        <#case "java.util.List<com.hazelcast.map.impl.querycache.event.QueryCacheEventData>">
            <#return "array of Query Cache Event Data">
        <#case "java.util.List<java.lang.String>">
            <#return "array of string">
        <#default>
            <#return "Unknown Data Type " + javaType>
    </#switch>
</#function>

