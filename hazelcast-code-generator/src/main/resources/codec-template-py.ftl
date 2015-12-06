from hazelcast.serialization.data import *
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.protocol.custom_codec import *
from hazelcast.protocol.codec.${util.convertToSnakeCase(model.parentName)}_message_type import *
<#if model.events?has_content>
from hazelcast.protocol.event_response_const import *
</#if>

REQUEST_TYPE = ${model.parentName?upper_case}_${model.name?upper_case}
RESPONSE_TYPE = ${model.response}
RETRYABLE = <#if model.retryable == 1>True<#else>False</#if>

<#--************************ REQUEST ********************************************************-->

def calculate_size(<#list model.requestParams as param>${util.convertToSnakeCase(param.name)}<#if param_has_next>, </#if></#list>):
    """ Calculates the request payload size"""
    data_size = 0
<#list model.requestParams as p>
    <@sizeText varName=util.convertToSnakeCase(p.name) type=p.type isNullable=p.nullable/>
</#list>
    return data_size


def encode_request(<#list model.requestParams as param>${util.convertToSnakeCase(param.name)}<#if param_has_next>, </#if></#list>):
    """ Encode request into client_message"""
    client_message = ClientMessage(payload_size=calculate_size(<#list model.requestParams as param>${util.convertToSnakeCase(param.name)}<#if param_has_next>, </#if></#list>))
    client_message.set_message_type(REQUEST_TYPE)
    client_message.set_retryable(RETRYABLE)
<#list model.requestParams as p>
<@setterText varName=util.convertToSnakeCase(p.name) type=p.type isNullable=p.nullable/>
</#list>
    client_message.update_frame_length()
    return client_message


<#--************************ RESPONSE ********************************************************-->
def decode_response(client_message):
    """ Decode response from client message"""
    parameters = dict(<#list model.responseParams as p>${util.convertToSnakeCase(p.name)}=None<#if p_has_next>, </#if></#list>)
<#list model.responseParams as p>
<@getterText varName=util.convertToSnakeCase(p.name) type=p.type isNullable=p.nullable indent=1/>
</#list>
    return parameters


<#--************************ EVENTS ********************************************************-->
<#if model.events?has_content>
def handle(client_message, <#list model.events as event>handle_event_${event.name?lower_case} = None<#if event_has_next>, </#if></#list>):
    """ Event handler """
    message_type = client_message.get_message_type()
    <#list model.events as event>
    if message_type == EVENT_${event.name?upper_case} and handle_event_${event.name?lower_case} is not None:
        <#list event.eventParams as p>
<@getterText varName=util.convertToSnakeCase(p.name) type=p.type isNullable=p.nullable isEvent=true indent=2/>
        </#list>
        handle_event_${event.name?lower_case}(<#list event.eventParams as param>${util.convertToSnakeCase(param.name)}<#if param_has_next>, </#if></#list>)
    </#list>
</#if>

<#--MACROS BELOW-->
<#--SIZE NULL CHECK MACRO -->
<#macro sizeText varName type isNullable=false>
<#if isNullable>
    data_size += BOOLEAN_SIZE_IN_BYTES
    if ${varName} is not None:
<@sizeTextInternal varName=varName type=type indent=2/>
<#else>
<@sizeTextInternal varName=varName type=type indent=1/>
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
<#macro sizeTextInternal varName type indent>
<#local cat= util.getTypeCategory(type)>
<#switch cat>
    <#case "OTHER">
        <#if util.isPrimitive(type)>
${""?left_pad(indent * 4)}data_size += ${type?upper_case}_SIZE_IN_BYTES
        <#else >
${""?left_pad(indent * 4)}data_size += calculate_size_${util.getPythonType(type)?lower_case}(${varName})
        </#if>
        <#break >
    <#case "CUSTOM">
${""?left_pad(indent * 4)}data_size += calculate_size_${util.getPythonType(type)?lower_case}(${varName})
        <#break >
    <#case "COLLECTION">
${""?left_pad(indent * 4)}data_size += INT_SIZE_IN_BYTES
        <#local genericType= util.getGenericType(type)>
        <#local n= varName>
${""?left_pad(indent * 4)}for ${varName}_item in ${varName}:
        <@sizeTextInternal varName="${n}_item"  type=genericType indent=(indent + 1)/>
        <#break >
    <#case "ARRAY">
${""?left_pad(indent * 4)}data_size += INT_SIZE_IN_BYTES
        <#local genericType= util.getArrayType(type)>
        <#local n= varName>
${""?left_pad(indent * 4)}for ${varName}_item in ${varName}:
        <@sizeTextInternal varName="${n}_item"  type=genericType indent=(indent + 1)/>
        <#break >
    <#case "MAP">
        <#local keyType = util.getFirstGenericParameterType(type)>
        <#local valueType = util.getSecondGenericParameterType(type)>
        <#local n= varName>
${""?left_pad(indent * 4)}for key, val in ${varName}.iteritems():
        <@sizeTextInternal varName="key"  type=keyType indent=(indent + 1)/>
        <@sizeTextInternal varName="val"  type=valueType indent=(indent + 1)/>
</#switch>
</#macro>

<#--SETTER NULL CHECK MACRO -->
<#macro setterText varName type isNullable=false>
<#local isNullVariableName= "${varName}_is_null">
<#if isNullable>
    client_message.append_bool(${varName} is None)
    if ${varName} is not None:
<@setterTextInternal varName=varName type=type indent=2/>
<#else>
<@setterTextInternal varName=varName type=type indent=1/>
</#if>
</#macro>

<#--SETTER MACRO -->
<#macro setterTextInternal varName type indent>
    <#local cat= util.getTypeCategory(type)>
    <#if cat == "OTHER">
${""?left_pad(indent * 4)}client_message.append_${util.getPythonType(type)?lower_case}(${varName})
    </#if>
    <#if cat == "CUSTOM">
${""?left_pad(indent * 4)}${util.getTypeCodec(type)?split(".")?last}.encode(client_message, ${varName})
    </#if>
    <#if cat == "COLLECTION">
${""?left_pad(indent * 4)}client_message.append_int(len(${varName}))
        <#local itemType= util.getGenericType(type)>
        <#local itemTypeVar= varName + "_item">
${""?left_pad(indent * 4)}for ${itemTypeVar} in ${varName}:
    <@setterTextInternal varName=itemTypeVar type=itemType indent=(indent + 1)/>
    </#if>
    <#if cat == "ARRAY">
${""?left_pad(indent * 4)}client_message.append_int(len(${varName}))
        <#local itemType= util.getArrayType(type)>
        <#local itemTypeVar= varName + "_item">
${""?left_pad(indent * 4)}for ${itemTypeVar} in ${varName}:
    <@setterTextInternal varName=itemTypeVar  type=itemType indent=(indent + 1)/>
    </#if>
    <#if cat == "MAP">
        <#local keyType = util.getFirstGenericParameterType(type)>
        <#local valueType = util.getSecondGenericParameterType(type)>
${""?left_pad(indent * 4)}client_message.append_int(len(${varName}))
${""?left_pad(indent * 4)}for key, value in ${varName}.iteritems():
    <@setterTextInternal varName="key"  type=keyType indent=(indent + 1)/>
    <@setterTextInternal varName="val"  type=valueType indent=(indent + 1)/>
    </#if>
</#macro>

<#--GETTER NULL CHECK MACRO -->
<#macro getterText varName type isNullable=false isEvent=false indent=1>
<#if isNullable>
${""?left_pad(indent * 4)}${varName}=None
${""?left_pad(indent * 4)}if not client_message.read_bool():
<@getterTextInternal varName=varName varType=type isEvent=isEvent indent=indent +1/>
<#else>
<@getterTextInternal varName=varName varType=type isEvent=isEvent indent= indent/>
</#if>
</#macro>

<#macro getterTextInternal varName varType isEvent indent>
<#local cat= util.getTypeCategory(varType)>
<#switch cat>
    <#case "OTHER">
        <#switch varType>
            <#case util.DATA_FULL_NAME>
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = client_message.read_data()
                <#break >
            <#case "java.lang.Integer">
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = client_message.read_int()
                <#break >
            <#case "java.lang.Boolean">
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = client_message.read_bool()
                <#break >
            <#case "java.lang.String">
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = client_message.read_str()
                <#break >
            <#case "java.util.Map.Entry<com.hazelcast.nio.serialization.Data,com.hazelcast.nio.serialization.Data>">
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = client_message.read_map_entry()
                <#break >
            <#default>
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = client_message.read_${util.getPythonType(varType)}()
        </#switch>
        <#break >
    <#case "CUSTOM">
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = ${util.getTypeCodec(varType)?split(".")?last}.decode(client_message)
        <#break >
    <#case "COLLECTION">
    <#case "ARRAY">
    <#local collectionType>java.util.ArrayList</#local>
    <#local itemVariableType= util.getGenericType(varType)>
    <#local itemVariableName= "${varName}_item">
    <#local sizeVariableName= "${varName}_size">
    <#local indexVariableName= "${varName}_index">
${""?left_pad(indent * 4)}${sizeVariableName} = client_message.read_int()
${""?left_pad(indent * 4)}${varName} = []
${""?left_pad(indent * 4)}for ${indexVariableName} in xrange(0, ${sizeVariableName}):
                            <@getterTextInternal varName=itemVariableName varType=itemVariableType isEvent=true indent=(indent +1)/>
${""?left_pad(indent * 4)}    ${varName}.append(${itemVariableName})
<#if !isEvent>
${""?left_pad(indent * 4)}parameters['${varName}'] = ${varName}
</#if>
        <#break >
    <#case "MAP">
        <#local sizeVariableName= "${varName}_size">
        <#local indexVariableName= "${varName}_index">
        <#local keyType = util.getFirstGenericParameterType(varType)>
        <#local valueType = util.getSecondGenericParameterType(varType)>
        <#local keyVariableName= "${varName}_key">
        <#local valVariableName= "${varName}_val">
${""?left_pad(indent * 4)}${sizeVariableName} = client_message.read_int()
${""?left_pad(indent * 4)}${varName} = {}
${""?left_pad(indent * 4)}for ${indexVariableName} in xrange(0,${sizeVariableName}):
            <@getterTextInternal varName=keyVariableName varType=keyType isEvent=true indent=(indent +1)/>
            <@getterTextInternal varName=valVariableName varType=valueType isEvent=true indent=(indent +1)/>
${""?left_pad(indent * 4)}    ${varName}[${keyVariableName}] = ${valVariableName}
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}'] = ${varName}</#if>
</#switch>
</#macro>