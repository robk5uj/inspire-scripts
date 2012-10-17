// ==UserScript==
// @name          Springer extract references
// @require       http://ajax.googleapis.com/ajax/libs/jquery/1.7.1/jquery.min.js
// ==/UserScript==

var count = 1;
var result = "";

$('li').each(function(index) {
	var x = $(this).find('.authors');
	if ( x.length ) {
		result += count + ". " + x.html() + "<br />";
		count += 1; 
	}
});

if (result != "") {
	var w = window.open();
	$(w.document.body).html(result);
}
else {
	alert("No references found")
}