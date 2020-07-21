ACTIVAGE Internet of Things (IoT) for ageing well www.activageproject.eu
Data Lake Query Execution component

This project has received funding from the European Union's Horizon 2020 
research and innovation programme under grant agreement No 732679.

Copyright 2020 Universitat Politècnica de València


I. Included Software

 - Swagger-UI (https://github.com/swagger-api/swagger-ui), Copyright 2018 SmartBear Software, distributed under Apache 2.0 license


II. Used Software

This product uses the following software with corresponding licenses:

<#function licenseFormat licenses>
    <#assign result = ""/>
    <#list licenses as license>
        <#assign result = result + license/>
        <#if license_has_next>
            <#assign result = result + " or "/>
        </#if>
    </#list>
    <#return result>
</#function>
<#function artifactFormat p>
    <#if p.name?index_of('Unnamed') &gt; -1>
        <#return p.artifactId + " (" + p.groupId + ":" + p.artifactId + ":" + p.version + " - " + (p.url!"no url defined") + ")">
    <#else>
        <#return p.name + " (" + p.groupId + ":" + p.artifactId + ":" + p.version + " - " + (p.url!"no url defined") + ")">
    </#if>
</#function>
<#if dependencyMap?size == 0>
The project has no dependencies.
<#else>
    <#list dependencyMap as e>
        <#assign project = e.getKey()/>
        <#assign licenses = e.getValue()/>
        ${artifactFormat(project)} distributed under ${licenseFormat(licenses)}
    </#list>
</#if>

II. License Summary

- Apache License, Version 2.0
- CDDL + GPLv2 with classpath exception
- Eclipse Public License - Version 1.0
- MIT License
