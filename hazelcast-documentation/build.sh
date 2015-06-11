#!/bin/bash

function buildDocumentation {
    init $1
    cleanIfExists
    copyImages
    createMultiHTML
    createMergedMarkdownFile
    createSingleHTML
    createMancenterDocumentation
    createPDF
    delete
    echo "Done"
}

function init {
	VERSION=$1
	OUTPUT_DIR="target"
	MULTI_HTML_OUTPUT_DIR="html"
	SINGLE_HTML_OUTPUT_DIR="html-single"
	MANCENTER_OUTPUT_DIR="mancenter"
	PDF_OUTPUT_DIR="pdf"
	PDF_FILE_NAME="hazelcast-documentation-${VERSION}.pdf"
	MANIFEST_FILE_NAME="manifest.json"
	MERGED_FILE_NAME="hazelcast-documentation.md"
	COPYRIGHT_FILE_NAME="copyright.txt"
	DATE=`date +%b\ %d\,\ %Y`
	YEAR=`date +%Y`
	INDEX=`awk '{gsub(/^[ \t]+|^([#]+.*)|[ \t]+([#]+.*)\$/,""); print;}' documentation.index`
	MANCENTER_INDEX=`awk '{gsub(/^[ \t]+|([#]+.*)|[ \t]+([#]+.*)\$/,""); print;}' mancenter.index`
}

function cleanIfExists {
	if [ -e "./$OUTPUT_DIR" ]; then
		echo "Cleaning $OUTPUT_DIR"
		$(rm -rf "./$OUTPUT_DIR")
	fi
	echo "Creating $OUTPUT_DIR"
	mkdir ${OUTPUT_DIR}
	echo "Creating $OUTPUT_DIR/$MULTI_HTML_OUTPUT_DIR"
	mkdir ${OUTPUT_DIR}/${MULTI_HTML_OUTPUT_DIR}
	echo "Creating $OUTPUT_DIR/$SINGLE_HTML_OUTPUT_DIR"
	mkdir ${OUTPUT_DIR}/${SINGLE_HTML_OUTPUT_DIR}
	echo "Creating $OUTPUT_DIR/$MANCENTER_OUTPUT_DIR"
	mkdir ${OUTPUT_DIR}/${MANCENTER_OUTPUT_DIR}
	echo "Creating $OUTPUT_DIR/$PDF_OUTPUT_DIR"
	mkdir ${OUTPUT_DIR}/${PDF_OUTPUT_DIR}
}

function copyImages {
    # BeautifulDocs is not working reliable when copying images, so we copy the files manually (bug is reported)
    if [ ! -d "./src/images" ]; then
        mkdir ./src/images
    fi
    cp -aR ./images/. ./src/images/.
    mkdir ./${OUTPUT_DIR}/${MULTI_HTML_OUTPUT_DIR}"/images/"
    mkdir ./${OUTPUT_DIR}/${SINGLE_HTML_OUTPUT_DIR}"/images/"
    mkdir ./${OUTPUT_DIR}/${MANCENTER_OUTPUT_DIR}"/images/"

    cp -aR ./images/ ./${OUTPUT_DIR}/${MULTI_HTML_OUTPUT_DIR}"/images/"
    cp -aR ./images/ ./${OUTPUT_DIR}/${SINGLE_HTML_OUTPUT_DIR}"/images/"
    cp -aR ./images/ ./${OUTPUT_DIR}/${MANCENTER_OUTPUT_DIR}"/images/"
}

function writeManifestFile {
    if [[ -e "./$MANIFEST_FILE_NAME" ]]; then
        $(rm -rf "./$MANIFEST_FILE_NAME")
    fi

    writeManifest=$( echo $1 >> ${MANIFEST_FILE_NAME})
    if [[ $? -eq 0 ]]; then
        echo "Manifest file succesfully written."
    else
        echo "Error writing manifest file"
        echo ${writeManifest}
        delete
        exit -1
    fi
}

function createMultiHTML {
    MANIFEST_FILE_BODY="{\"title\": \"Documentation\",
\"rootDir\": \".\",
\"date\": \"${DATE}\",
\"version\": \"${VERSION}\",
\"maxTocLevel\":3,
\"files\":"
    MANIFEST_FILE_BODY+="["

    echo "Building manifest file for multipage html"
    for file in ${INDEX}; do
        MANIFEST_FILE_BODY+="\"$file\","
    done
    MANIFEST_FILE_BODY=${MANIFEST_FILE_BODY:0: ${#MANIFEST_FILE_BODY}-1}
    MANIFEST_FILE_BODY+="]}"

    writeManifestFile "${MANIFEST_FILE_BODY}"

    echo "Creating multi_html documentation"
    createHtml=$(bfdocs --theme=themes/multi_html "./"${MANIFEST_FILE_NAME} "./"${OUTPUT_DIR}/${MULTI_HTML_OUTPUT_DIR})
    if [[ $? -ge 0 ]]; then
        echo "Multi HTML created successfully."
    else
        echo "Error creating Multi HTML documentation"
        delete
        exit -1
    fi
}

function createMergedMarkdownFile {
    if [[ -e "./$MERGED_FILE_NAME" ]]; then
	    $(rm -rf "./$MERGED_FILE_NAME")
    fi

    echo "Creating concatenated markdown file for pdf/single html."
    for file in ${INDEX}; do
        cat ${file} >> ${MERGED_FILE_NAME}
        printf "\n\n\n" >> ${MERGED_FILE_NAME}
    done
}

function createSingleHTML {
    MANIFEST_FILE_BODY="{\"title\": \"Documentation\",
\"rootDir\": \".\",
\"date\": \"${DATE}\",
\"version\": \"${VERSION}\",
\"maxTocLevel\":3,
\"files\":[\"./$MERGED_FILE_NAME\"]}"

    writeManifestFile "${MANIFEST_FILE_BODY}"

    echo "Creating single_html documentation"
    createHtml=$(bfdocs --theme=themes/single_html ${MANIFEST_FILE_NAME} "./"${OUTPUT_DIR}/${SINGLE_HTML_OUTPUT_DIR} )
    if [[ $? -eq 0 ]]; then
        echo "Single HTML created succesfully "
    else
        echo "Error creating Single HTML documentation"
        delete
        exit -1
    fi
}

function createMancenterDocumentation {
    MANIFEST_FILE_BODY="{\"title\": \"Documentation\",
\"rootDir\": \".\",
\"date\": \"${DATE}\",
\"version\": \"${VERSION}\",
\"maxTocLevel\":3,
\"files\":"
    MANIFEST_FILE_BODY+="["

    echo "Building manifest file for mancenter"
    for file in ${MANCENTER_INDEX}; do
        MANIFEST_FILE_BODY+="\"$file\","
    done
    MANIFEST_FILE_BODY=${MANIFEST_FILE_BODY:0: ${#MANIFEST_FILE_BODY}-1}
    MANIFEST_FILE_BODY+="]}"

    writeManifestFile "${MANIFEST_FILE_BODY}"

    echo "Creating Management Center documentation"
    createHtml=$(bfdocs --theme=themes/no_header ${MANIFEST_FILE_NAME} "./"${OUTPUT_DIR}/${MANCENTER_OUTPUT_DIR} )
    if [[ $? -eq 0 ]]; then
        echo "Management Center documentation created succesfully"
    else
        echo "Error creating Management Center documentation"
        delete
        exit -1
    fi
}

function createPDF {
    if [ -e "./$COPYRIGHT_FILE_NAME" ]; then
	    $(rm -rf "./$COPYRIGHT_FILE_NAME")
    fi
    echo "Preparing Copyright Text"
    printf "In-Memory Data Grid - Hazelcast | Documentation: version ${VERSION} \n\n" >> ${COPYRIGHT_FILE_NAME}
    printf "Publication date ${DATE}\n\n" >> ${COPYRIGHT_FILE_NAME}
    printf "Copyright © ${YEAR} Hazelcast, Inc.\n\n\n" >> ${COPYRIGHT_FILE_NAME}
    printf "Permission to use, copy, modify and distribute this document for any purpose and without fee is hereby granted in perpetuity, provided that the above copyright notice
and this paragraph appear in all copies." >> ${COPYRIGHT_FILE_NAME}
    echo "Copyright text created successfully"

    if [[ -e "./title.txt" ]]; then
        $(rm -rf "./title.txt")
    fi
    echo "Creating title page"
    echo "%Hazelcast Documentation" >> title.txt
    echo "%version "${VERSION} >> title.txt
    echo "%"${DATE} >> title.txt

    echo "Creating PDF Documentation"
    createPDF=$( pandoc title.txt ${MERGED_FILE_NAME} -o ${PDF_FILE_NAME} --toc --toc-depth=3 --chapters --number-sections --tab-stop=2 -V papersize:"a4paper" -H themes/margin.sty  --include-before-body=${COPYRIGHT_FILE_NAME} )
    if [[ $? -eq 0 ]]; then
        echo "PDF created successfully."
    else
        echo "Error creating PDF documentation"
        echo ${createPDF}
        delete
        exit -1
    fi

    mv ${PDF_FILE_NAME} ./${OUTPUT_DIR}/${PDF_OUTPUT_DIR}/${PDF_FILE_NAME}
}

function delete {
    echo "Deleting created files"
    $(rm -rf "./$COPYRIGHT_FILE_NAME")
    $(rm -rf "./$MERGED_FILE_NAME")
    $(rm -rf "./$MANIFEST_FILE_NAME")
    $(rm -rf "./title.txt")
    $(rm -rf "./src/images")
}

buildDocumentation $1
