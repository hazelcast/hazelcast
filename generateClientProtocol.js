var DATA_FULL_NAME = "com.hazelcast.nio.serialization.Data"
var BUILTIN_TYPES = [ "boolean", "byte" , "int",
                                           "java.lang.Integer",
                                           "java.lang.Long",
                                           "java.lang.String",
                                           "long" ]
var CUSTOM_CODECS = {"com.hazelcast.cache.impl.CacheEventData" : "CacheEventDataCodec" ,
"com.hazelcast.client.impl.client.DistributedObjectInfo" : "DistributedObjectInfoCodec" ,
"com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder": "EvictionConfigCodec",
"com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder" : "ListenerConfigCodec" ,
"com.hazelcast.client.impl.protocol.task.dynamicconfig.MapStoreConfigHolder" : "MapStoreConfigCodec" ,
"com.hazelcast.client.impl.protocol.task.dynamicconfig.NearCacheConfigHolder" : "NearCacheConfigCodec" ,
"com.hazelcast.client.impl.protocol.task.dynamicconfig.QueryCacheConfigHolder" : "QueryCacheConfigCodec" ,
"com.hazelcast.client.impl.protocol.task.dynamicconfig.QueueStoreConfigHolder" : "QueueStoreConfigCodec" ,
"com.hazelcast.client.impl.protocol.task.dynamicconfig.RingbufferStoreConfigHolder" : "RingbufferStoreConfigCodec" ,
"com.hazelcast.client.impl.protocol.task.dynamicconfig.PredicateConfigHolder" : "PredicateConfigCodec" ,
"com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig" : "TimedExpiryPolicyFactoryConfigCodec" ,
"com.hazelcast.config.CacheSimpleEntryListenerConfig": "CacheSimpleEntryListenerConfigCodec",
"com.hazelcast.config.HotRestartConfig" : "HotRestartConfigCodec" ,
"com.hazelcast.config.MapAttributeConfig" : "MapAttributeConfigCodec" ,
"com.hazelcast.config.MapIndexConfig" : "MapIndexConfigCodec" ,
"com.hazelcast.config.NearCachePreloaderConfig": "NearCachePreloaderConfigCodec" ,
"com.hazelcast.config.WanReplicationRef" : "WanReplicationRefCodec" ,
"com.hazelcast.core.Member" : "MemberCodec" ,
"com.hazelcast.map.impl.querycache.event.QueryCacheEventData" : "QueryCacheEventDataCodec" ,
"com.hazelcast.map.impl.SimpleEntryView": "EntryViewCodec" ,
"com.hazelcast.mapreduce.JobPartitionState" : "JobPartitionStateCodec" ,
"com.hazelcast.nio.Address" : "AddressCodec" ,
"com.hazelcast.scheduledexecutor.ScheduledTaskHandler" : "ScheduledTaskHandlerCodec" ,
"java.util.UUID": "UUIDCodec" ,
"java.util.Properties": "PropertiesCodec" ,
"javax.transaction.xa.Xid": "XIDCodec" }

function toBoolean(s){
    if(s == "true"){
       return true
    } else if(s == "false"){
        return false
    }
    console.log("not a boolean string")
}
function toString(t){
    if(t == 0)
        return "false"
    else
        return "true"
}

function readVariable(var_name, type, isNullable, isEvent, containsNullable=false){
    if(isNullable){
         isNullVariableName = var_name + "_isNull"
.        boolean @{isNullVariableName} = clientMessage.getBoolean();
.        if (!@{isNullVariableName}) {
.            @{getterTextInternal(var_name, type, containsNullable)}
         if(!isEvent){
.            parameters.@{var_name} = @{var_name};
         }
.        }
    } else {
.        @{getterTextInternal(var_name, type, containsNullable)}
         if(!isEvent){
.        parameters.@{var_name} = @{var_name};
         }
    }
.
}

function defineVariable(var_name, type){
.       @{type} @{var_name} = @{getDefaultValueForType(type)};
}

function getterText(var_name, type, isNullable=false, isEvent=false, containsNullable=false){
.        @{defineVariable(var_name, type)}
.        @{readVariable(var_name, type, isNullable, isEvent, containsNullable)}
}


 function getTypeCategory(type) {
        var endIndex = type.indexOf('<');
        var t = endIndex > 0 ? type.substring(0, endIndex) : type;
        if (t in CUSTOM_CODECS) {
            return "CUSTOM";
        } else if (t == "java.util.Map.Entry") {
            return "MAPENTRY";
        } else if (t == "java.util.List") {
            return "COLLECTION";
        } else if (type.endsWith("[]")) {
            return "ARRAY";
        }
        return "OTHER";
    }


function getTypeCodec(type) {
    var endIndex = type.indexOf('<');
    var t = endIndex > 0 ? type.substring(0, endIndex) : type;
    var codecType = CUSTOM_CODECS[t]
    if(codecType == ""){
        console.log("Not a custom codec : getTypeCodec")
    }
    return codecType
}


function getFirstGenericParameterType(type) {
    var beg = type.indexOf("<")
    var end = type.indexOf(",")
    return type.substring(beg + 1, end).trim()
}

function getSecondGenericParameterType(type) {
    var beg = type.indexOf(",")
    var end = type.lastIndexOf(">")
    return type.substring(beg + 1, end).trim()
}

function getGenericType(type) {
    var beg = type.indexOf("<");
    var end = type.lastIndexOf(">");
    return type.substring(beg + 1, end).trim();
}

function getArrayType(type) {
    var end = type.indexOf("[]");
    return type.substring(0, end).trim();
}

function capitalizeFirstLetter(word){
    return word[0].toUpperCase() + word.substr(1)
}

function getterTextInternal(var_name, varType, containsNullable=false){
    var category = getTypeCategory(varType)
    switch(category){
        case "OTHER":
            switch(varType){
                case DATA_FULL_NAME:
.               @{var_name} = clientMessage.getData();
                break;
                case "java.lang.Integer":
.               @{var_name} = clientMessage.getInt();
                break;
                case "java.lang.Long":
.               @{var_name} = clientMessage.getLong();
                break;
                case "java.lang.Boolean":
.               @{var_name} = clientMessage.getBoolean();
                break;
                case "java.lang.String":
.               @{var_name} = clientMessage.getStringUtf8();
                break;
                default:
.               @{var_name} = clientMessage.get@{capitalizeFirstLetter(varType)}();
            }
            break;
        case "CUSTOM":
.           @{var_name} = @{getTypeCodec(varType)}.decode(clientMessage);
        break;
        case "COLLECTION":
            var collectionType = "java.util.ArrayList"
            var itemVariableType = getGenericType(varType)
            var itemVariableName = var_name + "Item"
            var sizeVariableName = var_name + "Size"
            var indexVariableName = var_name + "Index"
            var isNullVariableName = itemVariableName + "IsNull"
.        int @{sizeVariableName} = clientMessage.getInt();
.        @{var_name} = new @{collectionType}<@{itemVariableType}>(@{sizeVariableName});
.        for (int @{indexVariableName} = 0; @{indexVariableName} < @{sizeVariableName}; @{indexVariableName}++) {
.            @{itemVariableType} @{itemVariableName};
            if(containsNullable){
.            @{itemVariableName} = null;
.            boolean @{isNullVariableName} = clientMessage.getBoolean();
.            if (!@{isNullVariableName}) {
.                @{getterTextInternal(itemVariableName, itemVariableType)}
.            }
            } else {
.             @{getterTextInternal(itemVariableName, itemVariableType)}
            }
.            @{var_name}.add(@{itemVariableName});
.        }
        break;
        case "ARRAY":
            var itemVariableType = getArrayType(varType)
            var itemVariableName = var_name + "_item"
            var sizeVariableName = var_name + "_size"
            var indexVariableName = var_name + "_index"
            var isNullVariableName = itemVariableName + "_isNull"
.        int @{sizeVariableName} = clientMessage.getInt();
.        @{var_name} = new @{itemVariableType}[@{sizeVariableName}];
.        for (int @{indexVariableName} = 0; @{indexVariableName} < @{sizeVariableName} ; @{indexVariableName}++) {
.            @{itemVariableType} @{itemVariableName};
            if(containsNullable){
.            @{itemVariableName} = null;
.            boolean @{isNullVariableName} = clientMessage.getBoolean();
.            if (!@{isNullVariableName}) {
.                @{getterTextInternal(itemVariableName, itemVariableType)}
.            }
            } else {
.             @{getterTextInternal(itemVariableName, itemVariableType)}
            }
.            @{var_name}[@{indexVariableName}] = @{itemVariableName};
.        }

        break;
        case "MAPENTRY":
            var sizeVariableName = var_name + "_size"
            var indexVariableName = var_name + "_index"
            var keyType = getFirstGenericParameterType(varType)
            var valueType = getSecondGenericParameterType(varType)
            var keyVariableName= var_name + "_key"
            var valVariableName= var_name + "_val"
.        @{keyType} @{keyVariableName};
.        @{valueType} @{valVariableName};
.        @{getterTextInternal(keyVariableName, keyType)}
.        @{getterTextInternal(valVariableName, valueType)}
.        @{var_name} = new java.util.AbstractMap.SimpleEntry<@{keyType},@{valueType}>(@{keyVariableName}, @{valVariableName});
         break;
         default:
            console.log("unrecognized type in getterTextInternal")
    }
}

function isPrimitive(type){
    var PRIMITIVE_TYPES = ["int", "long", "short", "byte", "boolean"];
    if(PRIMITIVE_TYPES.indexOf(type) != -1){
        return true;
    }
    return false;
}

function getDefaultValueForType(type){
    if(type == "boolean"){
.false
    } else if(isPrimitive(type)){
.0
    } else {
.null
    }
}

function setterTextInternal(var_name, type, containsNullable=false){
    var category = getTypeCategory(type)
    switch(category){
        case "OTHER":
.           clientMessage.set(@{var_name});
        break;
        case "CUSTOM":
.           @{getTypeCodec(type)}.encode(@{var_name}, clientMessage);
        break;
        case "COLLECTION":
.           clientMessage.set(@{var_name}.size());
            var itemType = getGenericType(type)
            var itemVarName = var_name + "_item"
.           for (@{itemType} @{itemVarName} : @{var_name}) {
.                @{setterText(itemVarName, itemType, containsNullable, false)}
.            }
        break;
        case "ARRAY":
.           clientMessage.set(@{var_name}.length);
            var itemType = getArrayType(type)
            var itemVarName = var_name + "_item"
.           for (@{itemType} @{itemVarName} : @{var_name}) {
.                @{setterText(itemVarName, itemType, containsNullable, false)}
.            }
        break;
        case "MAPENTRY":
            var keyType = getFirstGenericParameterType(type)
            var valueType = getSecondGenericParameterType(type)
            var keyName = var_name + "_key"
            var valName = var_name + "_val"
.        @{keyType} @{keyName} = @{var_name}.getKey();
.        @{valueType} @{valName} = @{var_name}.getValue();
.        @{setterTextInternal(keyName, keyType)}
.        @{setterTextInternal(valName, valueType)}
        break;
        default:
            console.log("Unrecognized type in setterTextInternal")
    }
}

function setterText(var_name, type, isNullable=false, containsNullable=false){
    if(isNullable){
         isNullVariableName = var_name + "IsNull"
.        boolean @{isNullVariableName};
.        if (@{var_name} == null) {
.            clientMessage.set(true);
.        } else {
.            clientMessage.set(false);
.            @{setterTextInternal(var_name, type, containsNullable)}
.        }
    } else {
.        @{setterTextInternal(var_name, type, containsNullable)}
    }
.
}

function methodParam(type){
    var category = getTypeCategory(type)
    switch(category){
        case "COLLECTION":
            var genericType = getGenericType(type);
.            java.util.Collection<@{genericType}>
            break;
        default:
.            @{type}
    }
}

function sizeTextInternal(var_name, type, containsNullable=false){
    var category = getTypeCategory(type)
    switch(category){
        case "OTHER":
        if(isPrimitive(type)){
.           dataSize += Bits.@{type.toUpperCase()}_SIZE_IN_BYTES;
        } else {
.           dataSize += ParameterUtil.calculateDataSize(@{var_name});
        }
        break;
        case "CUSTOM":
.           dataSize += @{getTypeCodec(type)}.calculateDataSize(@{var_name});
        break;
        case "COLLECTION":
.           dataSize += Bits.INT_SIZE_IN_BYTES;
            var genericType = getGenericType(type)
.           for (@{genericType} @{var_name}_item : @{var_name} ) {
.               @{sizeText(var_name + "_item", genericType, containsNullable, false)}
.           }
        break;
        case "ARRAY":
.           dataSize += Bits.INT_SIZE_IN_BYTES;
            var genericType = getArrayType(type)
.           for (@{genericType} @{var_name}_item : @{var_name} ) {
.               @{sizeText(var_name + "_item", genericType, containsNullable, false)}
.           }
        break;
        case "MAPENTRY":
            var keyType = getFirstGenericParameterType(type)
            var valueType = getSecondGenericParameterType(type)
            var keyName = var_name + "_key"
            var valName = var_name + "_val"
.           @{keyType} @{keyName} = @{var_name}.getKey();
.           @{valueType} @{valName} = @{var_name}.getValue();
.           @{sizeText(keyName, keyType, false, false)}
.           @{sizeText(valName, valueType, false , false)}
        break;
        default:
           console.log("unrecognized type in sizeTextInternal")
        break;
    }
}

function sizeText(name, type, isNullable, containsNullable){
    if(isNullable){
.        dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
.        if (@{name} != null) {
.            @{sizeTextInternal(name, type, containsNullable)}
.        }
    } else {
.        @{sizeTextInternal(name, type, containsNullable)}
    }
.
}

function typeNameParams(params){
./!separate(", ")
    for(var i = 0; i < params.length; i++){
        var param = params[i]
.        /+@{methodParam(param.type)} @{param.name}
    }
.
}

function nameParams(params){
./!separate(", ")
    for(var i = 0; i < params.length; i++){
        var param = params[i]
.        /+@{param.name}
    }
.
}

function requestPart(item){
    if (!("requests" in item)){
        item.requests = []
    }
.    //************************ REQUEST *************************//
.
.    public static class RequestParameters {
     for(var i = 0; i < item.requests.length; i++){
         var param = item.requests[i]

.        public @{param.type} @{param.name};
         if(parseFloat(param.since) > parseFloat(item.since)){
.        public boolean @{param.name}Exist = false;
         }
     }
.
.        public static int calculateDataSize(@{typeNameParams(item.requests)}) {
.            int dataSize = ClientMessage.HEADER_SIZE;
             for(var i = 0; i < item.requests.length; i++){
                 var param = item.requests[i]
.            @{sizeText(param.name, param.type, toBoolean(param.nullable), toBoolean(param.containsNullable))}
             }
.            return dataSize;
.        }
.    }
.
.    public static ClientMessage encodeRequest(@{typeNameParams(item.requests)}) {
.        final int requiredDataSize = RequestParameters.calculateDataSize(@{nameParams(item.requests)});
.        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
.        clientMessage.setMessageType(REQUEST_TYPE);
.        clientMessage.setRetryable(RETRYABLE);
    for(var i = 0; i < item.requests.length; i++){
        var param = item.requests[i]
.        @{setterText(param.name, param.type, toBoolean(param.nullable), toBoolean(param.containsNullable))}
    }
.        clientMessage.updateFrameLength();
.        return clientMessage;
.    }
.
.    public static RequestParameters decodeRequest(ClientMessage clientMessage) {
.        final RequestParameters parameters = new RequestParameters();
         previousVersion = "1.0"
     for(var i = 0; i < item.requests.length; i++){
         var param = item.requests[i]
         if(previousVersion != param.since){
.        if (clientMessage.isComplete()) {
.            return parameters;
.        }
         }

         // Id 2: AuthenticationCodec, Id:3 CustomAuthenticationCodec
         if( param.name == "clientHazelcastVersion" && (item.requestId == "0x0002" || item.requestId == "0x0003")){
.        try {
.             @{getterText(param.name, param.type, toBoolean(param.nullable), false, toBoolean(param.containsNullable))}
.        } catch (IndexOutOfBoundsException e) {
.            if ("CSP".equals(parameters.clientType)) {
.                // suppress this error for older csharp client since they had a bug which was fixed later (writeByte related)
.                return parameters;
.            } else {
.                throw e;
.            }
.        }
         }else {
.        @{getterText(param.name, param.type, toBoolean(param.nullable), false, toBoolean(param.containsNullable))}
         }
         if(parseFloat(param.since) > parseFloat(item.since)){
.        parameters.@{param.name}Exist = true;
         }
         previousVersion = param.since
    }
.        return parameters;
.    }
.
}

function responsePart(item){
    if (!("response" in item)){
        item.response = {"params" : [] }
    }

.//************************ RESPONSE *************************//
.
.    public static class ResponseParameters {
     for(var i = 0; i < item.response.params.length; i++){
         var param = item.response.params[i]
.        public @{param.type} @{param.name};
        if(parseFloat(param.since) > parseFloat(item.since)){
.        public boolean @{param.name}Exist = false;
        }
.
    }
.        public static int calculateDataSize(@{typeNameParams(item.response.params)}) {
.            int dataSize = ClientMessage.HEADER_SIZE;
    for(var i = 0; i < item.response.params.length; i++){
             var param = item.response.params[i]
.            @{sizeText(param.name, param.type, toBoolean(param.nullable), toBoolean(param.containsNullable))}
    }
.            return dataSize;
.        }
.    }
.

.    public static ClientMessage encodeResponse(@{typeNameParams(item.response.params)}) {
.        final int requiredDataSize = ResponseParameters.calculateDataSize(@{nameParams(item.response.params)});
.        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
.        clientMessage.setMessageType(RESPONSE_TYPE);
    for(var i = 0; i < item.response.params.length; i++){
                 var param = item.response.params[i]
.        @{setterText(param.name, param.type, toBoolean(param.nullable), toBoolean(param.containsNullable))}
    }
.        clientMessage.updateFrameLength();
.        return clientMessage;
.
.    }
.
.    public static ResponseParameters decodeResponse(ClientMessage clientMessage) {
.        ResponseParameters parameters = new ResponseParameters();
    previousVersion = "1.0"
    for(var i = 0; i < item.response.params.length; i++){
        var param = item.response.params[i]
        if(previousVersion != param.since){
.        if (clientMessage.isComplete()) {
.            return parameters;
.        }
         }
.        @{getterText(param.name, param.type, toBoolean(param.nullable), false, toBoolean(param.containsNullable))}
        if(parseFloat(param.since) > parseFloat(item.since)){
.        parameters.@{param.name}Exist = true;
        }
        previousVersion = param.since
    }
.        return parameters;
.    }
.
}

function eventsPart(item){
    if(item.event == null){
        return
    }
.
.    //************************ EVENTS *************************//
.

    for(var i = 0; i < item.event.length; i++){
        var event = item.event[i]
.    public static ClientMessage encode@{event.name}Event(@{typeNameParams(event.params)}){
.        int dataSize = ClientMessage.HEADER_SIZE;
        for(var j = 0; j < event.params.length; j++){
            var param = event.params[j]
.        @{sizeText(param.name, param.type, toBoolean(param.nullable), false)}
        }
.
.        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
.        clientMessage.setMessageType(@{event.eventId});
.        clientMessage.addFlag(ClientMessage.LISTENER_EVENT_FLAG);
.
        for(var j = 0; j < event.params.length; j++){
            var param = event.params[j]
.        @{setterText(param.name, param.type, toBoolean(param.nullable), false)}
        }
.        clientMessage.updateFrameLength();
.        return clientMessage;
.    };
.
    }
.
.  public static abstract class AbstractEventHandler{
.
.        public void handle(ClientMessage clientMessage) {
.            int messageType = clientMessage.getMessageType();
    for(var i = 0; i < item.event.length; i++){
        var event = item.event[i]
.            if (messageType == @{event.eventId}){
.                boolean messageFinished = false;
        previousVersion = "1.0"
        for(var j = 0; j < event.params.length; j++){
            var param = event.params[j]
.                @{defineVariable(param.name, param.type)}
            if(previousVersion != param.since){
.                if (!messageFinished) {
.                    messageFinished = clientMessage.isComplete();
.                }
             }
.                if (!messageFinished) {
.                    @{readVariable(param.name, param.type, toBoolean(param.nullable), true, false)}
.                }
           previousVersion = param.since
         }
.                handle(@{nameParams(event.params)});
.                return;
.            }
    }
.            com.hazelcast.logging.Logger.getLogger(super.getClass()).warning("Unknown message type received on event handler :" + clientMessage.getMessageType());
.        }
.
    for(var i = 0; i < item.event.length; i++){
            var event = item.event[i]
.        public abstract void handle(@{typeNameParams(event.params)});
    }
.   }
.
}

var data = require("./hazelcast/src/main/resources/protocol.json")
console.log("Printing ...")
for(var i = 0; i < data.length; i++){
console.log(i + " of " + data.length)
    item = data[i]
./!output("./hazelcast/src/main/java/com/hazelcast/client/impl/protocol/codec/" + item.fullName + ".java")
./*
. * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
. *
. * Licensed under the Apache License, Version 2.0 (the "License");
. * you may not use this file except in compliance with the License.
. * You may obtain a copy of the License at
. *
. * http://www.apache.org/licenses/LICENSE-2.0
. *
. * Unless required by applicable law or agreed to in writing, software
. * distributed under the License is distributed on an "AS IS" BASIS,
. * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
. * See the License for the specific language governing permissions and
. * limitations under the License.
. */
.//DO NOT MODIFY. THIS FILE IS GENERATED. TO MODIFY IT  generateClientProtocol.js
.package com.hazelcast.client.impl.protocol.codec;
.
.import com.hazelcast.client.impl.protocol.ClientMessage;
.import com.hazelcast.client.impl.protocol.util.ParameterUtil;
.import com.hazelcast.nio.Bits;
.import javax.annotation.Generated;
.
.@Generated("Hazelcast.code.generator")
.@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
.public final class @{item.fullName} {
//    if(item.fullName != "MapGetEntryViewCodec") continue;
.
.    public static final int REQUEST_TYPE = @{item.requestId};
.    public static final int RESPONSE_TYPE = @{item.responseId};
.    public static final boolean RETRYABLE = @{toString(item.retryable)};
.
    requestPart(item)
    responsePart(item)
    eventsPart(item)
.}
}
console.log("Done ...")
