#Protocol Messages

## Compound Data Types Used In The Protocol Specification
Some common compound data structures used in the protocol message specification are defined in this section.

### Array
In the protocol specification an array of a data type is frequently used. An array of a data type with n entries are encode in this way:

|Field|Type|Nullable|Description|
|-----|----|---------|----------|
|Length|int32|No|The length of the array. In this example, it shall be a value of n.|
|Entry 1|provided data type|No|First entry of the array|
|Entry 2|provided data type|No|Second entry of the array|
|...|...|...|...|
|...|...|...|...|
|Entry n|provided data type|No|n'th entry of the array|

### Address Data Type
| Field| Type| Nullable| Description|
|------|-----|---------|------------|
|Host|string|No|The name or the ip address of the server member|
|Port|int32|No|The port number used for this address|

### Cache Event Data Type
| Field| Type| Nullable| Description|
|------|-----|---------|------------|
|Cache Event type|int32|No|The type of the event. Possible values and their meanings are:<br> CREATED(1):An event type indicating that the cache entry was created. <br>UPDATED(2): An event type indicating that the cache entry was updated, i.e. a previous mapping existed. <br>REMOVED(3): An event type indicating that the cache entry was removed. <br>EXPIRED(4): An event type indicating that the cache entry has expired.<br>EVICTED(5): An event type indicating that the cache entry has evicted. <br>INVALIDATED(6): An event type indicating that the cache entry has invalidated for near cache invalidation. <br>COMPLETED(7): An event type indicating that the cache operation has completed. <br>EXPIRATION_TIME_UPDATED(8): An event type indicationg that the expiration time of cache record has been updated
|Name|string|No|Name of the cache|
|Key|byte-array|No|Key of the cache data|
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

### Member Attribute Change Data Type
| Field| Type| Nullable| Description|
|------|-----|---------|------------|
|uuid|string|No|Unique user id of the member server|
|key|string|No|Name of the attribute changed|
|operation Type|int32|No|Type of the change. Possible values are: <br>1: An attribute is added <br>2: An attribute is removed|
|Value|string|No|Value of the attribute. This field only exist for operation type of 1, otherwise this field is not transferred at all|

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

    <#if cm.requestParams?has_content>

| Name| Type| Nullable| Description|
|-----|-----|---------|------------|
        <#list cm.requestParams as param>
|${param.name}| ${util.convertTypeToDocumentType(param.type)}| <#if param.nullable >Yes<#else>No</#if>|${util.getDescription(param.name, cm.comment)}|
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
|${param.name}| ${util.convertTypeToDocumentType(param.type)}| <#if param.nullable >Yes<#else>No</#if>|
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
|${param.name}| ${util.convertTypeToDocumentType(param.type)}| <#if param.nullable >Yes<#else>No</#if>|${util.getDescription(param.name, cm.comment)}|
        </#list>
    <#else>

Header only event message, no message body exist.
    </#if>

</#list>

    </#if>

</#list>
</#if>
</#list>

