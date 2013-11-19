function delete {
	echo "Deleting created files"
	$(rm -rf "./$MANIFEST_FILE_NAME")
	$(rm -rf "./src/$MERGED_FILE_NAME")
	$(rm -rf "./$COPYRIGHT_FILE_NAME")
	$(rm -rf "./title.txt")
	$(rm -rf "./src/images")
}

function createMultiHTML {

MANIFEST_FILE_BODY="{\"title\": \"Documentation\",
\"rootDir\": \".\",
\"maxTocLevel\":3,
\"files\":"
MANIFEST_FILE_BODY+="["

echo "Building manifest file for multipage html"
fileList=$(ls src/*.md)
for file in $fileList
do
MANIFEST_FILE_BODY+="\""$file"\","
done
MANIFEST_FILE_BODY=${MANIFEST_FILE_BODY:0: ${#MANIFEST_FILE_BODY}-1}
MANIFEST_FILE_BODY+="]}"

if [[ -e "./$MANIFEST_FILE_NAME" ]]; then
	$(rm -rf "./$MANIFEST_FILE_NAME")
fi
writeManifest=$( echo $MANIFEST_FILE_BODY >> "./"$MANIFEST_FILE_NAME)
if [[ $? -ge 0 ]]; then
  echo "Manifest file succesfully written."
else
  echo "Error writing manifest file"
  delete
  exit -1
fi

echo "Creating multi_html documentation"

createHtml=$(bfdocs --theme=themes/multi_html "./"$MANIFEST_FILE_NAME "./"$OUTPUT_DIR/$MULTI_HTML_OUTPUT_DIR)
if [[ $? -ge 0 ]]; then
  echo "Multi HTML created successfully."
else
  echo "Error creating Multi HTML documentation"
  delete
  exit -1
fi
}

function createPDF {

if [ -e "./$COPYRIGHT_FILE_NAME" ]; then
	$(rm -rf "./$COPYRIGHT_FILE_NAME")
fi


echo "Preparing Copyright Text"

COPYRIGHT_TEXT="In-Memory Data Grid - Hazelcast | Documentation: version $version \n\n" 
date=`date +%d\ %b\ %Y`
COPYRIGHT_TEXT+="Publication date $date \n\n"
year=`date +%Y`
COPYRIGHT_TEXT+="Copyright © $year Hazelcast, Inc.\n\n\n"
COPYRIGHT_TEXT+="Permission to use, copy, modify and distribute this document for any purpose and without fee is hereby granted in perpetuity, provided that the above copyright notice
and this paragraph appear in all copies."

if [ -e "./$COPYRIGHT_FILE_NAME" ]; then
	$(rm -rf "./$COPYRIGHT_FILE_NAME")
fi

echo -e $COPYRIGHT_TEXT >> $COPYRIGHT_FILE_NAME

echo "Copyright text created successfully"




if [[ -e "./$MERGED_FILE_NAME" ]]; then
	$(rm -rf "./$MERGED_FILE_NAME")
fi

echo "Creating concatenated markdown file for pdf/single html."

for file in `ls src/*.md`
do
 cat $file >> $MERGED_FILE_NAME
 printf "\n" >> $MERGED_FILE_NAME
done 

if [[ -e "./title.txt" ]]; then
	$(rm -rf "./title.txt")
fi

echo "Creating title page"
echo "%Hazelcast Documentation" >> title.txt
date=`date +%b\ %d\,\ %Y`
echo "%version "$version >> title.txt
echo "%"$date >> title.txt

echo "Creating PDF Documentation"

createPDF=$( pandoc title.txt $MERGED_FILE_NAME -o $PDF_FILE_NAME --toc --toc-depth=3 --chapters --number-sections --tab-stop=2 -V papersize:"a4paper" -H themes/margin.sty  --include-before-body=$COPYRIGHT_FILE_NAME )

if [[ $? -eq 0 ]]; then
  echo "PDF created successfully."
else
  echo "Error creating PDF documentation"
  echo $createPDF
  delete
  exit -1
fi


}

function createSingleHTML {

MANIFEST_FILE_BODY="{\"title\": \"Documentation\",
\"rootDir\": \".\",
\"maxTocLevel\":3,
\"files\":[\"./src/$MERGED_FILE_NAME\"]}" 

if [[ -e "./$MANIFEST_FILE_NAME" ]]; then
	$(rm -rf "./$MANIFEST_FILE_NAME")
fi
writeManifest=$( echo $MANIFEST_FILE_BODY >> $MANIFEST_FILE_NAME)
if [[ $? -eq 0 ]]; then
  echo "Manifest file succesfully written."
else
  echo "Error writing manifest file"
  echo $writeManifest
  delete
  exit -1
fi

echo "Creating single_html documentation"

createHtml=$(bfdocs --theme=themes/single_html $MANIFEST_FILE_NAME "./"$OUTPUT_DIR/$SINGLE_HTML_OUTPUT_DIR )
if [[ $? -eq 0 ]]; then
  echo "Single HTML created succesfully "
  
else
  echo "Error creating Single HTML documentation"
  exit -1
  delete
fi 
}

function init {
	OUTPUT_DIR="target"
	MULTI_HTML_OUTPUT_DIR="multi_html"
	SINGLE_HTML_OUTPUT_DIR="single_html"
	PDF_FILE_NAME="hazelcast-documentation.pdf"
	MANIFEST_FILE_NAME="manifest.json"
	MERGED_FILE_NAME="hazelcast-documentation.md"
	COPYRIGHT_FILE_NAME="copyright.txt"
	
}

function cleanIfExists {
	if [ -e "./$OUTPUT_DIR" ]; then
		echo "Cleaning $OUTPUT_DIR"
		$(rm -rf "./$OUTPUT_DIR")
	fi
	echo "Creating $OUTPUT_DIR"
	mkdir $OUTPUT_DIR
	echo "Creating $OUTPUT_DIR/$MULTI_HTML_OUTPUT_DIR"
	mkdir $OUTPUT_DIR/$MULTI_HTML_OUTPUT_DIR	
	echo "Creating $OUTPUT_DIR/$SINGLE_HTML_OUTPUT_DIR"
	mkdir $OUTPUT_DIR/$SINGLE_HTML_OUTPUT_DIR	
		
}

init
version=$1
cleanIfExists
createMultiHTML

createPDF
mv $PDF_FILE_NAME ./$OUTPUT_DIR/$PDF_FILE_NAME
echo "Move merged file from $MERGED_FILE_NAME to /src/$MERGED_FILE_NAME"
mv $MERGED_FILE_NAME ./src/$MERGED_FILE_NAME
#########
mkdir ./src/images
cp -a ./images/*.jpg ./src/images/
mkdir ./$OUTPUT_DIR/$SINGLE_HTML_OUTPUT_DIR"/images/"
createSingleHTML

# Move images manually, BeautifulDocs is not working reliable when copying images. Bug reported about that.
cp -a ./images/*.jpg ./$OUTPUT_DIR/$MULTI_HTML_OUTPUT_DIR"/images/"
cp -a ./images/*.jpg ./$OUTPUT_DIR/$SINGLE_HTML_OUTPUT_DIR"/images/"
delete

echo "Done"








