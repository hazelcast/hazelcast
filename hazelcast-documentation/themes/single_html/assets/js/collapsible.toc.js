function makeTocCollapsible() {
 // Get the top TOC UL element
 var tocdiv = xGetElementById("toc"); if (!tocdiv) return;
 var toc = GetFirstElementByTagName("ul",tocdiv); if (!toc) return;
 toc.id = "toctop";

 // scan only the child nodes of top UL element, not all LI elements
 for (var i = 0; i < toc.childNodes.length; i++) {
  var ulElem = toc.childNodes[i];
  if (ulElem.nodeName.toLowerCase() != "li") continue;
  
  // get the first link in the level-1 bullet line and modify it
  var aElem = GetFirstElementByTagName("a",ulElem);
  if (!aElem) continue;
  aElem.onclick = ExpandOrCollapse; aElem.onclick();
 }
}