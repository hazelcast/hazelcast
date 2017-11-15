#!/usr/bin/env node

/*

  Copyright (c) 2014-2015 Contributors as noted in AUTHORS file.

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"),
  to deal in the Software without restriction, including without limitation
  the rights to use, copy, modify, merge, publish, distribute, sublicense,
  and/or sell copies of the Software, and to permit persons to whom
  the Software is furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included
  in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
  IN THE SOFTWARE.

*/

var PROLOGUE = "#!/usr/bin/env node\n\
\n\
/*\n\
\n\
  Copyright (c) 2014-2015 Contributors as noted in AUTHORS file.\n\
\n\
  Permission is hereby granted, free of charge, to any person obtaining a copy\n\
  of this software and associated documentation files (the \"Software\"),\n\
  to deal in the Software without restriction, including without limitation\n\
  the rights to use, copy, modify, merge, publish, distribute, sublicense,\n\
  and/or sell copies of the Software, and to permit persons to whom\n\
  the Software is furnished to do so, subject to the following conditions:\n\
\n\
  The above copyright notice and this permission notice shall be included\n\
  in all copies or substantial portions of the Software.\n\
\n\
  THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR\n\
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,\n\
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL\n\
  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER\n\
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING\n\
  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS\n\
  IN THE SOFTWARE.\n\
\n\
*/\n\
\n\
//Small changes to Basic classes to make life easier.\n\
Array.prototype.last = function() {\n\
    return this[this.length - 1];\n\
}\n\
\n\
function Ribosome() {\n\
\n\
    var fs = require('fs');\n\
\n\
    function Block(s) {\n\
        var self = this;\n\
        this.text = [];\n\
        this.width = 0;\n\
\n\
        if (s != null) {\n\
            this.text = s.split('\\n');\n\
            this.text.forEach(function(line) {\n\
                self.width = Math.max(self.width, line.length);\n\
            });\n\
        }\n\
\n\
        this.add_right = function(block) {\n\
            var i = 0;\n\
            var self = this;\n\
            block.text.forEach(function(line) {\n\
                if (self.text.length > i) {\n\
                    self.text[i] = self.text[i] +\n\
                        Array(self.width - self.text[i].length + 1).join(' ') + line;\n\
                } else {\n\
                    self.text[i] = Array(self.width + 1).join(' ') + line;\n\
                }\n\
                i++;\n\
            });\n\
            this.width += block.width;\n\
\n\
        };\n\
\n\
        this.add_bottom = function(block) {\n\
            this.text = this.text.concat(block.text);\n\
            this.width = Math.max(this.width, block.width);\n\
\n\
        };\n\
\n\
        this.trim = function() {\n\
\n\
            var top = -1;\n\
            var bottom = -1;\n\
            var left = -1;\n\
            var right = -1;\n\
\n\
            this.text.forEach(function(line, index) {\n\
\n\
                if (line.trim() != '') {\n\
                    if (top == -1) {\n\
                        top = index;\n\
                    }\n\
                    bottom = index;\n\
                    if (left == -1) {\n\
                        left = line.length - (line + 'W').trim().length + 1;\n\
                    } else {\n\
                        left = Math.min(left, line.length - (line + 'W').trim().length + 1);\n\
                    }\n\
                    if (right == -1) {\n\
                        right = ('W' + line).trim().length - 1;\n\
                    } else {\n\
                        right = Math.max(right, ('W' + line).trim().length - 1);\n\
                    }\n\
\n\
                }\n\
\n\
            });\n\
\n\
            if (bottom == -1) {\n\
                this.text = [];\n\
                this.width = 0;\n\
                return;\n\
            }\n\
\n\
            this.text = this.text.slice(top, bottom + 1);\n\
\n\
            this.text.forEach(function(line, index, array) {\n\
                array[index] = line.slice(left, right);\n\
            });\n\
\n\
            this.width = right - left;\n\
\n\
        };\n\
\n\
        this.write = function(out, outisafile, tabsize) {\n\
            this.text.forEach(function(line) {\n\
\n\
                if (tabsize > 0) {\n\
                    var ws = line.length - (line + 'w').trim().length + 1;\n\
                    var line = Array(Math.floor(ws / tabsize) + 1).join('\t') +\n\
                        Array((ws % tabsize) + 1).join(' ') + (line + 'W').trim().slice(0, -1);\n\
                }\n\
                if (outisafile == true) {\n\
                    fs.appendFileSync(out, line);\n\
                    fs.appendFileSync(out, '\\n');\n\
                } else {\n\
                    out.write(line);\n\
                    out.write('\\n');\n\
                }\n\
            });\n\
\n\
        };\n\
\n\
        this.last_offset = function() {\n\
            if (this.text.length == 0) {\n\
                return 0;\n\
            } else {\n\
                var last = this.text[this.text.length - 1];\n\
                return last.length - (last + \"w\").trim().length + 1;\n\
            }\n\
        };\n\
\n\
    }\n\
\n\
    var tabsize = 0;\n\
\n\
    var outisafile = false;\n\
    var out = process.stdout;\n\
    var append_flag = false;\n\
\n\
    var stack = [\n\
        []\n\
    ];\n\
\n\
    this.output = output;\n\
\n\
    function output(filename) {\n\
        close();\n\
        outisafile = true;\n\
        append_flag = false;\n\
        out = filename;\n\
    };\n\
\n\
    this.append = append;\n\
\n\
    function append(filename) {\n\
        close();\n\
        outisafile = true;\n\
        append_flag = true;\n\
        out = filename;\n\
    };\n\
\n\
    this.stdout = stdout;\n\
\n\
    function stdout() {\n\
        close();\n\
        outisafile = false;\n\
        out = process.stdout;\n\
    };\n\
\n\
    this.tabsize = change_tabsize;\n\
\n\
    function change_tabsize(size) {\n\
        tabsize = size;\n\
    };\n\
\n\
    this.close = close;\n\
\n\
    function close() {\n\
        if (append_flag == false && typeof out === \"string\") {\n\
            if (fs.existsSync(out)) {\n\
                fs.unlinkSync(out);\n\
            }\n\
        }\n\
        stack.last().forEach(function(b) {\n\
            b.write(out, outisafile, tabsize);\n\
        });\n\
        stack = [\n\
            []\n\
        ];\n\
    }\n\
\n\
    this.add = add;\n\
\n\
    function add(line, leval) {\n\
\n\
        if (stack.last().length == 0) {\n\
            stack.last().push(new Block(''));\n\
        }\n\
\n\
        var block = stack.last().last();\n\
\n\
        var i = 0;\n\
\n\
        while (true) {\n\
            var j = line.substr(i).search(/[@&][1-9]?\\{/);\n\
            if (j == -1) {\n\
                j = line.length;\n\
            } else {\n\
                j += i;\n\
            }\n\
\n\
            if (i != j) {\n\
                block.add_right(new Block(line.slice(i, j)));\n\
            }\n\
            if (j == line.length) {\n\
                break;\n\
            }\n\
\n\
            i = j;\n\
            j++;\n\
\n\
            var level = parseInt(line.charAt(j), 10);\n\
            if (isNaN(level)) {\n\
                level = 0;\n\
            } else {\n\
                j++;\n\
            }\n\
\n\
            var par = 0;\n\
\n\
            while (true) {\n\
                if (line.charAt(j) == '{') {\n\
                    par++;\n\
                } else {\n\
                    if (line.charAt(j) == '}') {\n\
                        par--;\n\
                    }\n\
                }\n\
\n\
                if (par == 0) {\n\
                    break;\n\
                }\n\
                j++;\n\
\n\
                if (j >= line.length) {\n\
                    process.stderr.write('SyntaxError: Unmatched {');\n\
                }\n\
            }\n\
\n\
            if (level > 0) {\n\
                if (line.charAt(i + 1) == '1') {\n\
                    block.add_right(new Block('@' + line.slice(i + 2, j + 1)));\n\
                } else {\n\
                    line = line.slice(0, i + 1) + (parseInt(line.charAt(i + 1)) - 1) + line.slice(i + 2);\n\
                    block.add_right(new Block(line.slice(i, j + 1)));\n\
                }\n\
                i = j + 1;\n\
                continue;\n\
            }\n\
\n\
            //TODO level can only be zero here.\n\
            var expr = line.slice((level == 0) ? i + 2 : i + 3, j);\n\
\n\
            stack.push([]);\n\
            var val = leval(expr);\n\
            var top = stack.pop();\n\
            if (top.length == 0) {\n\
                val = new Block(val.toString());\n\
            } else {\n\
                val = new Block('');\n\
                top.forEach(function(b) {\n\
                    val.add_bottom(b);\n\
                });\n\
            }\n\
\n\
            if (line.charAt(i) == '@') {\n\
                val.trim();\n\
            }\n\
            block.add_right(val);\n\
            i = j + 1;\n\
\n\
        }\n\
\n\
    }\n\
\n\
    this.dot = dot;\n\
\n\
    function dot(line, leval) {\n\
        stack.last().push(new Block(''));\n\
        add(line, leval);\n\
    }\n\
\n\
    this.align = align;\n\
\n\
    function align(line, leval) {\n\
        var n;\n\
        if (stack.last().length == 0) {\n\
            n = 0;\n\
        } else {\n\
            n = stack.last().last().last_offset();\n\
        }\n\
\n\
        stack.last().push(new Block(''));\n\
\n\
        add(Array(n + 1).join(' '));\n\
        add(line, leval);\n\
    }\n\
\n\
    this.rethrow = rethrow;\n\
\n\
    function rethrow(e, linemap) {\n\
\n\
        var new_msg=[];\n\
        var msg = e.stack.split(\"\\n\");\n\
        new_msg.push(msg[0]);\n\
        for (var i = 1; i < msg.length; i++) {\n\
            if (msg[i].indexOf(\".rna:\") != -1) {\n\
                var lindexes = msg[i].split(\".rna:\")[1].split(\":\");\n\
                var rrow = parseInt(lindexes[1]);\n\
                var rna_column = parseInt(lindexes[0]);\n\
                var rcolumn = 0;\n\
                var filename;\n\
                for (var j = 0; j < linemap.length - 1; j++) {\n\
                    if (linemap[j][0] <= rna_column) {\n\
                        filename = linemap[j][1];\n\
                    } else {\n\
                        break;\n\
                    }\n\
                }\n\
                j--\n\
                rcolumn = rna_column - linemap[j][0] + linemap[j][2];\n\
                var new_line = msg[i].replace(/\\(.*\\)$/, \"(\" + filename + \":\" + rcolumn + \":\" + rrow + \")\");\n\
		if (new_line != msg[i]) {\n\
		    new_msg.push(new_line);\n\
		}\n\
            }\n\
        }\n\
        new_msg.forEach(function(item) {\n\
            process.stderr.write(item);\n\
            process.stderr.write(\"\\n\");\n\
        });\n\
\n\
        process.exit(1);\n\
    }\n\
\n\
}\n\
\n\
var ribosome = new Ribosome();\n\
\n\
var at = \"@\";\n\
var amp = \"&\";\n\
var slash = \"/\";\n\
\n\
///////////////////////////////////////////////////////////////////////\n\
//\n\
//  The code that belongs to the protein project ends at this point of the\n\
//  RNA file and so does the associated license. What follows is the code\n\
//  generated from the DNA file.\n\
//\n\
///////////////////////////////////////////////////////////////////////\n\
"

//Small changes to Basic classes to make life easier.
Array.prototype.last = function() {
    return this[this.length - 1];
}

var fs = require('fs');
var path = require('path');
var exec = require('child_process').exec;


function addslashes(str) {
    return (str + '').replace(/[\\"']/g, '\\$&').replace(/\u0000/g, '\\0');
}



function usage() {
    process.stderr.write("usage: ribosome.js [options] <dna-file> <args-passed-to-dna-script>\n");
    process.exit(1);
}

function dnaerror(s) {
    process.stderr.write(dnastack.last()[1] + ":" + dnastack.last()[2] + " - " + s + "\n");
    process.exit(1);
}


function rnawrite(s) {
    linemap.push([rnaln, dnastack.last()[1], dnastack.last()[2]]);
    if (rnaopt) {
        process.stdout.write(s);
    } else {
        fs.appendFileSync(rnafile, s);
    }
    var m = s.match(/\n/g);
    rnaln += m == null ? 0 : m.length;
}

if (process.argv.length < 3 || process.argv[2] == "-h" ||
      process.argv[2] == "--help") {
    usage();
}
if (process.argv[2] == "-v" || process.argv[2] == "--version") {
    process.stderr.write("ribosome code generator, version 1.17\n");
    process.exit(1);
}

var rnaopt;
var dnafile;
if (process.argv[2] == "--rna") {
    if (process.argv.length < 4) {
        usage();
    }

    rnaopt = true;
    dnafile = process.argv[3];

} else {
    rnaopt = false;
    dnafile = process.argv[2];
}

var dnastack = [
    [null, "ribosome.js", 27, ""]
];

if (!rnaopt) {
    if (dnafile.slice(-4) == ".dna") {
        rnafile = dnafile.slice(0, -4) + ".rna";
    } else {
        rnafile = dnafile + ".rna";
    }
}
rnaln = 1;
linemap = [];

try {
    fs.unlinkSync(rnafile)
}
catch (e) {
}
rnawrite(PROLOGUE);
rnawrite('\n\n//-------------Begin-------------\n\n');

if (!rnaopt) {
    rnawrite('try {\n');
}

var eol = /\r?\n/;
var dirname = path.normalize(path.dirname(dnafile));
var file;
try {
    file = fs.readFileSync(dnafile, "utf8");
} catch (e) {
    process.stderr.write("The file \n'" + dnafile + "'\n doesn't exist.");
    process.exit(1);
}

dnastack.push([file.split(eol), dnafile, 0, dirname]);

while (dnastack.length > 1) {

    var line = dnastack.last()[0][0];
    dnastack.last()[0].shift();
    dnastack.last()[2] ++;

    if (dnastack.last()[0].length == 0) {
        dnastack.pop();
    }


    if ((line.length == 0) || (line[0] != ".")) {
        rnawrite(line + '\n');
        continue;
    }

    line = line.slice(1);

    if (line.slice(-1) == "$") {
        line = line.slice(0, -1);
    }

    if (line.indexOf("\t") != -1) {
        dnaerror("tab found in the line, replace it by space");
    }

    var firsttwo = line.trim().slice(0, 2);

    if (firsttwo == "/+") {
        rnawrite("ribosome.add('" + addslashes((line + 'w').trim().slice(2, -1)) + "',function(_expr){return eval(_expr);});\n");
        continue;
    }

    if (firsttwo == "/=") {
        rnawrite("ribosome.align('" + addslashes((line + 'w').trim().slice(2, -1)) + "',function(_expr){return eval(_expr);});\n");
        continue;
    }

    if (firsttwo == "/!") {
        line = (line + 'w').trim().slice(2, -1);
        var match = line.match(/^[0-9A-Za-z_]+/);
        if (match == null) {
            dnaerror("/! should be followed by an identifier");
        }

        var command = match[0];

        if (["output", "append", "stdout", "tabsize"].indexOf(command) >= 0) {
            rnawrite("ribosome." + line + "\n");
            continue;
        }

        if (command == "separate") {
            var separator = line.match(/["'].*["']/);
            if (separator == null) {
                dnaerror("Bad command syntax");
            }
            separator = separator[0].slice(1, -1);
            var cname = "___separate_" + rnaln + "___";
            rnawrite("var " + cname + " = true;\n");
            line = dnastack.last()[0][0];
            dnastack.last()[0].shift();
            dnastack.last()[2] += 1;
            if (line == null || line[0] == "." ||
                (!line.indexOf("while") && !line.indexOf("for"))) {
                dnaerror("'separate' command must be followed by a loop.");
            }

            rnawrite(line);
            rnawrite("\nif(" + cname + ") {\n")
            rnawrite("    " + cname + " = false;\n")
            rnawrite("} else {\n")
            rnawrite("    ribosome.add('" + addslashes(separator) + "',function(_expr){return eval(_expr);});\n")
            rnawrite("}\n")
            continue;

        }
        if (command == "include") {
            var filename = line.match(/["'].*["']/);
            if (filename == null) {
                dnaerror("Bad command syntax");
            }
            filename = filename[0].slice(1, -1).trim();
            filename = path.normalize(path.join(dnastack.last()[3], filename));
            var dirname = path.dirname(filename);

            var file;
            try {
                file = fs.readFileSync(filename, "utf8");
            } catch (e) {
                dnaerror("File doesn't exist.");
            }
            dnastack.push([file.split(eol), filename, 0, dirname]);
            continue;
        }

        dnaerror("Unknown command " + command);

    }

    rnawrite('ribosome.dot("' + addslashes(line) + '",function(_expr){return eval(_expr);})\n');
}


if (!rnaopt) {
    fs.appendFileSync(rnafile, "}catch(e){\n");

    fs.appendFileSync(rnafile, "var LINEMAP = [\n");
    var last = null;
    linemap.forEach(function(le) {
        if (last == null || le[1] != last[1] || le[0] - last[0] != le[2] - last[2]) {
            fs.appendFileSync(rnafile, "[" +
                le[0] + ",'" + addslashes(le[1]) + "'," + le[2] + "],\n");
            last = le;
        }
    });

    fs.appendFileSync(rnafile, "        [null]\n");
    fs.appendFileSync(rnafile, "    ];\n");
    fs.appendFileSync(rnafile, "ribosome.rethrow(e, LINEMAP);\n");


    fs.appendFileSync(rnafile, "}\n");

}

if (rnaopt) {
    process.stdout.write("ribosome.close();\n\n");
} else {
    fs.appendFileSync(rnafile, "ribosome.close();\n\n");
}

if (!rnaopt) {
    exec("node" + rnafile + " " + process.argv.slice(3).join(' '), function(error, stdout, stderr) {
        if(stderr != "") {
            exec("node " + rnafile + " " + process.argv.slice(3).join(' '), function(error, stdout, stderr) {
                process.stdout.write(stdout);
                process.stdout.write(stderr);
                fs.unlinkSync(rnafile);

                if (error) {
                    process.exit(error.code);
                }
            });
        } else {
            process.stdout.write(stdout);
            fs.unlinkSync(rnafile);
        }
    });
}

