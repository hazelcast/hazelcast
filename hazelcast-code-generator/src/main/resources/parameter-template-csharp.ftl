using System;

namespace Hazelcast.Client.Protocol.${model.parentName?cap_first}
{
    internal class ${model.className}
    {

        public static readonly ${model.parentName?cap_first}MessageType Type = ${model.parentName?cap_first}MessageType.${model.parentName?upper_case}_${model.name?upper_case};
<#list model.params as param>
        public ${param.type} ${param.name?cap_first} {get;set;}
</#list>


        private ${model.className}(ClientMessage clientMessage)
        {
<#list model.params as param>
            ${param.name?cap_first} = clientMessage.${param.dataGetterString}();
</#list>
        }

        public static ${model.className} Decode(ClientMessage clientMessage)
        {
            return new ${model.className}(clientMessage);
        }

        public static ClientMessage Encode(<#list model.params as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>)
        {
            final int requiredDataSize = CalculateDataSize(<#list model.params as param>${param.name}<#if param_has_next>, </#if></#list>);
            ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
            clientMessage.ensureCapacity(requiredDataSize);
            clientMessage.setMessageType(Type.id());
<#list model.params as param>
            clientMessage.Set(${param.name?cap_first});
</#list>
            clientMessage.updateFrameLenght();
            return clientMessage;
        }

        public static int CalculateDataSize(<#list model.params as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>)
        {
            return ClientMessage.HEADER_SIZE//
<#list model.params as param>
                + ${param.sizeString}
</#list>;
        }


    }
}
