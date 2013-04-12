<?xml version="1.0" encoding="utf-8"?>
<!--
  ~ Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
  ~
  ~ Licensed under the Apache License, Version 2.0 (the "License");
  ~ you may not use this file except in compliance with the License.
  ~ You may obtain a copy of the License at
  ~
  ~ http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:xslthl="http://xslthl.sf.net"
                exclude-result-prefixes="xslthl" version='1.0'>

    <!-- import the main stylesheet, here pointing to fo/docbook.xsl -->
    <xsl:import href="urn:docbkx:stylesheet"/>

    <!-- highlight.xsl must be imported in order to enable highlighting support,
         highlightSource=1 parameter is not sufficient -->
    <xsl:import href="urn:docbkx:stylesheet/highlight.xsl"/>

    <xsl:param name="highlight.source" select="1"/>
    <xsl:param name="admon.graphics" select="1"/>
    <xsl:param name="chapter.autolabel" select="1"/>
    <xsl:param name="section.autolabel" select="1"/>
    <xsl:param name="section.label.includes.component.label" select="1"/>

    <!-- some customization -->

    <xsl:template name="user.header.content">
        <xsl:copy-of select="document('./../../../src/main/docbook/manual/stylesheet/tracker.js',/)"/>
    </xsl:template>

    <xsl:template match='xslthl:string' mode="xslthl">
        <font font-style="normal" color="#008000">
            <xsl:apply-templates mode="xslthl"/>
        </font>
    </xsl:template>

    <!-- 	<xsl:template match='xslthl:keyword' mode="xslthl"> -->
    <!-- 		<font face="arial" color="#0A1777" font-weight="bold"> -->
    <!-- 			<xsl:apply-templates mode="xslthl" /> -->
    <!-- 		</font> -->
    <!-- 	</xsl:template> -->
    <!-- 	<xsl:template match='xslthl:comment' mode="xslthl"> -->
    <!-- 		<font font-style="italic" color="gray"> -->
    <!-- 			<xsl:apply-templates mode="xslthl" /> -->
    <!-- 		</font> -->
    <!-- 	</xsl:template> -->
    <!-- 	<xsl:template match='xslthl:multiline-comment' mode="xslthl"> -->
    <!-- 		<font font-style="italic" color="gray"> -->
    <!-- 			<xsl:apply-templates mode="xslthl" /> -->
    <!-- 		</font> -->
    <!-- 	</xsl:template> -->
    <!-- 	<xsl:template match='xslthl:number' mode="xslthl"> -->
    <!-- 		<font font-style="italic" color="#FF3311"> -->
    <!-- 			<xsl:apply-templates mode="xslthl" /> -->
    <!-- 		</font> -->
    <!-- 	</xsl:template> -->
    <xsl:template match='xslthl:tag' mode="xslthl">
        <font font-style="bold" color="#0A1777">
            <xsl:apply-templates mode="xslthl"/>
        </font>
    </xsl:template>
    <xsl:template match='xslthl:attribute' mode="xslthl">
        <font font-style="bold" color="#0A1777">
            <xsl:apply-templates mode="xslthl"/>
        </font>
    </xsl:template>
    <xsl:template match='xslthl:value' mode="xslthl">
        <font font-style="bold" color="#008000">
            <xsl:apply-templates mode="xslthl"/>
        </font>
    </xsl:template>

    <!-- <xsl:attribute-set name="monospace.verbatim.properties"> <xsl:attribute
         name="font-family">Lucida Sans Typewriter</xsl:attribute> <xsl:attribute
         name="font-size">9pt</xsl:attribute> <xsl:attribute name="keep-together.within-column">always</xsl:attribute>
         </xsl:attribute-set> <xsl:param name="shade.verbatim" select="1"/> <xsl:attribute-set
         name="shade.verbatim.style"> <xsl:attribute name="background-color">#f0f0f0</xsl:attribute>
         <xsl:attribute name="border-width">0.5pt</xsl:attribute> <xsl:attribute name="border-style">solid</xsl:attribute>
         <xsl:attribute name="border-color">#f0f0f0</xsl:attribute> <xsl:attribute
         name="padding">3pt</xsl:attribute> </xsl:attribute-set> -->
</xsl:stylesheet>