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
                xmlns:fo="http://www.w3.org/1999/XSL/Format" xmlns:xslthl="http://xslthl.sf.net"
                exclude-result-prefixes="xslthl" version='1.0'>

    <!-- import the main stylesheet, here pointing to fo/docbook.xsl -->
    <xsl:import href="urn:docbkx:stylesheet"/>

    <!-- highlight.xsl must be imported in order to enable highlighting support,
         highlightSource=1 parameter is not sufficient -->
    <xsl:import href="urn:docbkx:stylesheet/highlight.xsl"/>

    <xsl:param name="highlight.source" select="1"/>
    <xsl:param name="highlight.default.language" select="xml"/>
    <xsl:param name="admon.graphics" select="1"/>
    <xsl:param name="chapter.autolabel" select="1"/>
    <xsl:param name="section.autolabel" select="1"/>
    <xsl:param name="section.label.includes.component.label" select="1"/>

    <xsl:param name="page.margin.inner">10mm</xsl:param>
    <xsl:param name="page.margin.outer">10mm</xsl:param>
    <xsl:param name="alignment">left</xsl:param>
    <xsl:param name="symbol.font.family" select="'Symbol,ZapfDingbats,Monospaced'"/>

    <!-- some customization -->
    <xsl:template match='xslthl:keyword' mode="xslthl">
        <fo:inline font-weight="bold" color="#0A1777">
            <xsl:apply-templates mode="xslthl"/>
        </fo:inline>
    </xsl:template>
    <xsl:template match='xslthl:comment' mode="xslthl">
        <fo:inline font-style="italic" color="#7C7C7C">
            <xsl:apply-templates mode="xslthl"/>
        </fo:inline>
    </xsl:template>
    <xsl:template match='xslthl:multiline-comment' mode="xslthl">
        <fo:inline font-style="italic" color="#7C7C7C">
            <xsl:apply-templates mode="xslthl"/>
        </fo:inline>
    </xsl:template>
    <xsl:template match='xslthl:string' mode="xslthl">
        <fo:inline font-style="normal" color="#008000">
            <xsl:apply-templates mode="xslthl"/>
        </fo:inline>
    </xsl:template>
    <xsl:template match='xslthl:number' mode="xslthl">
        <fo:inline font-style="italic" color="#1930D1">
            <xsl:apply-templates mode="xslthl"/>
        </fo:inline>
    </xsl:template>
    <xsl:template match='xslthl:tag' mode="xslthl">
        <fo:inline font-style="normal" color="#0A1777">
            <xsl:apply-templates mode="xslthl"/>
        </fo:inline>
    </xsl:template>
    <xsl:template match='xslthl:attribute' mode="xslthl">
        <fo:inline font-style="bold" color="#0A1777">
            <xsl:apply-templates mode="xslthl"/>
        </fo:inline>
    </xsl:template>
    <xsl:template match='xslthl:value' mode="xslthl">
        <fo:inline font-style="bold" color="#008000">
            <xsl:apply-templates mode="xslthl"/>
        </fo:inline>
    </xsl:template>

    <xsl:attribute-set name="monospace.verbatim.properties">
        <xsl:attribute name="font-family">Monospaced</xsl:attribute>
        <xsl:attribute name="font-size">8pt</xsl:attribute>
        <xsl:attribute name="keep-together.within-column">always</xsl:attribute>
    </xsl:attribute-set>
    <xsl:param name="shade.verbatim" select="1"/>
    <xsl:attribute-set name="shade.verbatim.style">
        <xsl:attribute name="background-color">#f0f0f0</xsl:attribute>
        <xsl:attribute name="border-width">0.5pt</xsl:attribute>
        <xsl:attribute name="border-style">solid</xsl:attribute>
        <xsl:attribute name="border-color">#f0f0f0</xsl:attribute>
        <xsl:attribute name="padding">3pt</xsl:attribute>
    </xsl:attribute-set>
    <xsl:attribute-set name="component.title.properties">
        <xsl:attribute name="font-family">
            <xsl:value-of select="$title.font.family"/>
        </xsl:attribute>
        <xsl:attribute name="font-weight">bold</xsl:attribute>
        <xsl:attribute name="font-size">20pt</xsl:attribute>
        <xsl:attribute name="keep-with-next.within-column">always</xsl:attribute>
        <xsl:attribute name="text-align">left</xsl:attribute>
        <xsl:attribute name="space-before.minimum">0.8em</xsl:attribute>
        <xsl:attribute name="space-before.optimum">1.0em</xsl:attribute>
        <xsl:attribute name="space-before.maximum">1.2em</xsl:attribute>
    </xsl:attribute-set>
    <xsl:attribute-set name="section.title.level1.properties">
        <xsl:attribute name="font-size">16pt</xsl:attribute>
    </xsl:attribute-set>
    <xsl:attribute-set name="section.title.level2.properties">
        <xsl:attribute name="font-size">12pt</xsl:attribute>
    </xsl:attribute-set>
</xsl:stylesheet>