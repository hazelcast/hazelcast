import {ClientMessage} from "../ClientMessage";
import {BitsUtil} from '../BitsUtil';
import {CustomCodec} from './CustomCodec';
import {${model.parentName}MessageType} from './${model.parentName}MessageType';

var REQUEST_TYPE = ${model.parentName}MessageType.${model.parentName?upper_case}_${model.name?upper_case}
var RESPONSE_TYPE = ${model.response}
var RETRYABLE = <#if model.retryable == 1>true<#else>false</#if>

<#--************************ REQUEST ********************************************************-->

export class ${model.className}{

constructor() {
}




static calculateSize(<#list model.requestParams as param>${util.convertToNodeType(param.name)}<#if param_has_next>, </#if></#list>){
    // Calculates the request payload size
    var dataSize = 0;
<#list model.requestParams as p>
    <@sizeText varName=util.convertToNodeType(p.name) type=p.type isNullable=p.nullable/>
</#list>
    return dataSize;
}

static encodeRequest(<#list model.requestParams as param>${util.convertToNodeType(param.name)}<#if param_has_next>, </#if></#list>){
    // Encode request into client_message
    var payloadSize;
    var clientMessage = new ClientMessage(payloadSize=calculateSize(<#list model.requestParams as param>${util.convertToNodeType(param.name)}<#if param_has_next>, </#if></#list>));
    clientMessage.setMessageType(REQUEST_TYPE);
    clientMessage.setRetryable(RETRYABLE);
<#list model.requestParams as p>
<@setterText varName=util.convertToNodeType(p.name) type=p.type isNullable=p.nullable/>
</#list>
    clientMessage.updateFrameLength();
    return clientMessage;
}

<#--************************ RESPONSE ********************************************************-->
static decodeResponse(clientMessage){
    // Decode response from client message
    var parameters;
<#list model.responseParams as p>
<@getterText varName=util.convertToNodeType(p.name) type=p.type isNullable=p.nullable/>
</#list>
    return parameters;

}

}
<#--************************ EVENTS ********************************************************-->
<#if model.events?has_content>
static handle(clientMessage, <#list model.events as event>handle_event_${event.name?lower_case} = None<#if event_has_next>, </#if></#list>):
    """ Event handler """
    message_type = clientMessage.get_message_type()
    <#list model.events as event>
    if ( message_type === EVENT_${event.name?upper_case} && handle_event_${event.name?lower_case} !== null) {
        <#list event.eventParams as p>
<@getterText varName=util.convertToSnakeCase(p.name) type=p.type isNullable=p.nullable isEvent=true/>
        </#list>
        handle_event_${event.name?lower_case}(<#list event.eventParams as param>${util.convertToSnakeCase(param.name)}<#if param_has_next>, </#if></#list>)
    }
    </#list>
</#if>

<#--MACROS BELOW-->
<#--SIZE NULL CHECK MACRO -->
<#macro sizeText varName type isNullable=false>
<#if isNullable>
    dataSize += BOOLEAN_SIZE_IN_BYTES;
    if(${varName} !== null) {
<@sizeTextInternal varName=varName type=type/>
    }
<#else>
<@sizeTextInternal varName=varName type=type/>
</#if>
</#macro>


<#--METHOD PARAM MACRO -->
<#macro methodParam type><#local cat= util.getTypeCategory(type)>
<#switch cat>
<#case "COLLECTION"><#local genericType= util.getGenericType(type)>java.util.Collection<${genericType}><#break>
<#default>${type}
</#switch>
</#macro>

<#--SIZE MACRO -->
<#macro sizeTextInternal varName type>
<#local cat= util.getTypeCategory(type)>
<#switch cat>
    <#case "OTHER">
        <#if util.isPrimitive(type)>
    dataSize += ${type?upper_case}_SIZE_IN_BYTES;
        <#else >
    dataSize += BitsUtil.calculateSize${util.capitalizeFirstLetter(util.getNodeType(type)?lower_case)}(${varName});
        </#if>
        <#break >
    <#case "CUSTOM">
    dataSize += BitsUtil.calculateSize${util.capitalizeFirstLetter(util.getNodeType(type)?lower_case)}(${varName});
        <#break >
    <#case "COLLECTION">
    dataSize += INT_SIZE_IN_BYTES;
        <#local genericType= util.getGenericType(type)>
        <#local n= varName>
    for( ${varName}Item in ${varName}){
        <@sizeTextInternal varName="${n}Item"  type=genericType />
    }
        <#break >
    <#case "ARRAY">
    data_size += INT_SIZE_IN_BYTES
        <#local genericType= util.getArrayType(type)>
        <#local n= varName>
    for( ${varName}Item in ${varName}){
        <@sizeTextInternal varName="${n}Item"  type=genericType />
    }
        <#break >
    <#case "MAP">
        <#local keyType = util.getFirstGenericParameterType(type)>
        <#local valueType = util.getSecondGenericParameterType(type)>
        <#local n= varName>
    for( entry in ${varName}){
        <@sizeTextInternal varName="entry.key"  type=keyType />
        <@sizeTextInternal varName="entry.val"  type=valueType />
    }
</#switch>
</#macro>

<#--SETTER NULL CHECK MACRO -->
<#macro setterText varName type isNullable=false>
<#local isNullVariableName= "${varName}IsNull">
<#if isNullable>
    clientMessage.appendBool(${varName} === null);
    if(${varName} !== null){
<@setterTextInternal varName=varName type=type />
    }
<#else>
<@setterTextInternal varName=varName type=type />
</#if>
</#macro>

<#--SETTER MACRO -->
<#macro setterTextInternal varName type >
    <#local cat= util.getTypeCategory(type)>
    <#if cat == "OTHER">
    clientMessage.append${util.capitalizeFirstLetter(util.capitalizeFirstLetter(util.getNodeType(type)?lower_case))}(${varName});
    </#if>
    <#if cat == "CUSTOM">
    ${util.getTypeCodec(type)?split(".")?last}.encode(client_message, ${varName});
    </#if>
    <#if cat == "COLLECTION">
    clientMessage.appendInt(len(${varName}))
        <#local itemType= util.getGenericType(type)>
        <#local itemTypeVar= varName + "Item">
    for( ${itemTypeVar} in ${varName}) {
    <@setterTextInternal varName=itemTypeVar type=itemType />
    }
    </#if>
    <#if cat == "ARRAY">
    clientMessage.appendInt(len(${varName}));
        <#local itemType= util.getArrayType(type)>
        <#local itemTypeVar= varName + "Item">
    for( ${itemTypeVar} in ${varName}){
    <@setterTextInternal varName=itemTypeVar  type=itemType />
    }
    </#if>
    <#if cat == "MAP">
        <#local keyType = util.getFirstGenericParameterType(type)>
        <#local valueType = util.getSecondGenericParameterType(type)>
    clientMessage.appendInt(len(${varName}))
    for( entry in ${varName}){
    <@setterTextInternal varName="entry.key"  type=keyType />
    <@setterTextInternal varName="entry.val"  type=valueType />
    }
    </#if>
</#macro>

<#--GETTER NULL CHECK MACRO -->
<#macro getterText varName type isNullable=false isEvent=false >
<#if isNullable>

if(clientMessage.readBool() === true){
<@getterTextInternal varName=varName varType=type isEvent=isEvent />
}
<#else>
<@getterTextInternal varName=varName varType=type isEvent=isEvent />
</#if>
</#macro>

<#macro getterTextInternal varName varType isEvent >
<#local cat= util.getTypeCategory(varType)>
<#switch cat>
    <#case "OTHER">
        <#switch varType>
            <#case util.DATA_FULL_NAME>
    <#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.readData();
                <#break >
            <#case "java.lang.Integer">
    <#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.readInt();
                <#break >
            <#case "java.lang.Boolean">
    <#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.readBool();
                <#break >
            <#case "java.lang.String">
    <#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.readStr();
                <#break >
            <#case "java.util.Map.Entry<com.hazelcast.nio.serialization.Data,com.hazelcast.nio.serialization.Data>">
    <#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.readMapEntry();
                <#break >
            <#default>
    <#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.read${util.capitalizeFirstLetter(util.getNodeType(varType))}();
        </#switch>
        <#break >
    <#case "CUSTOM">
    <#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = ${util.getTypeCodec(varType)?split(".")?last}.decode(client_message)
        <#break >
    <#case "COLLECTION">
    <#case "ARRAY">
    <#local collectionType>java.util.ArrayList</#local>
    <#local itemVariableType= util.getGenericType(varType)>
    <#local itemVariableName= "${varName}Item">
    <#local sizeVariableName= "${varName}Size">
    <#local indexVariableName= "${varName}Index">
    ${sizeVariableName} = client_message.read_int()
    ${varName} = [];
    for(var ${indexVariableName} = 0 ;  ${indexVariableName} <= ${sizeVariableName} ; ${indexVariableName}++){
                            <@getterTextInternal varName=itemVariableName varType=itemVariableType isEvent=true />
        ${varName}.push(${itemVariableName})
    }
<#if !isEvent>
    parameters['${varName}'] = ${varName}
</#if>
        <#break >
    <#case "MAP">
        <#local sizeVariableName= "${varName}Size">
        <#local indexVariableName= "${varName}Index">
        <#local keyType = util.getFirstGenericParameterType(varType)>
        <#local valueType = util.getSecondGenericParameterType(varType)>
        <#local keyVariableName= "${varName}Key">
        <#local valVariableName= "${varName}Val">
    ${sizeVariableName} = client_message.read_int()
    ${varName} = {}
    for ${indexVariableName} in xrange(0,${sizeVariableName}):
            <@getterTextInternal varName=keyVariableName varType=keyType isEvent=true />
            <@getterTextInternal varName=valVariableName varType=valueType isEvent=true />
        ${varName}[${keyVariableName}] = ${valVariableName}
        <#if !isEvent>parameters['${varName}'] = ${varName}</#if>
</#switch>
</#macro>