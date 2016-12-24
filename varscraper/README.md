# varscraper

Tool for periodically fetch `/debug/vars` on a server.

The go expvar library exports metrics over `/debug/vars` in JSON
format.  This script fetches the specified metrics and writes them out
in CSV format.

The CSV output can be imported into a spreadsheet for graphing.