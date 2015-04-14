package ${model.packageName};

public enum ${model.className} {

<#list model.params as param>
    ${model.name?upper_case}_${param.name?upper_case}(${param.id})<#if param_has_next>,<#else>;</#if>
</#list>

    private final int id;

    ${model.className}(int messageType) {
        this.id = messageType;
    }

    public int id() {
        return id;
    }


}


