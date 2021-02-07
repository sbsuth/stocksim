var fs = require('fs');
const util = require('util');
const csv = require('csv-parser')
const { Transform } = require('stream');
var dataDir = ".";

function setDataDir(dir) {
    dataDir = dir;
}

function csvPath(ticker) {
    return dataDir + "/" + ticker + ".csv";
}

function pctString(num,prec) {
    if (prec == undefined) {
        prec = 0;
    }
    return parseFloat(num*100).toFixed(prec)+"%";
}


// A transform stream that produces a set of intervals for a StockSource.
// The StockSource defines a ticker, start date, end date, and frequency.
class StockIntervals extends Transform {
  constructor(stock) {
    super( {
        writableObjectMode : true,
        readableObjectMode : true,
        transform( chunk, encoding, callback ) { this.transform(chunk,encoding,callback ) }
    })
    this.params = {
        startDate: stock.start ? new Date(stock.start) : undefined,
        endDate: stock.end ? new Date(stock.endDate) : undefined,
        frequency : stock.freq ? stock.freq : "daily"
    };
    this.lastDate = undefined;
    this.startData = undefined;
    this.startDate = undefined;
  }
  transform( chunk, encoding, callback ) {
        var date = new Date(Date.parse(chunk['Date']));
        if (   ((this.params.startDate == undefined) || (date >= this.params.startDate))
            && ((this.params.endDate == undefined) || (date <= this.params.endDate))) {
            if ((this.lastDate != undefined) && (this.lastData != undefined)) {
                if (this.params.frequency == "daily") {
                    this.startDate = date;
                    this.startData = chunk;
                    this.push( this.getData( date, chunk) );
                } else if (this.params.frequency == "monthly") {

                    if (this.lastDate.getMonth() != date.getMonth()) {
                        if (this.startData != undefined) {
                            this.push( this.getData( this.lastDate, this.lastData ) );
                        }
                        this.startData = chunk;
                        this.startDate = date;
                    }
                } else if (this.params.frequency == "weekly") {
                    if (this.lastDate.getDay() > date.getDay()) {
                        // Sunday is 0, so if day of last date is larger than this day, we have a week start.
                        if (this.startData != undefined) {
                            this.push( this.getData( this.lastDate, this.lastData ) );
                        }
                        this.startData = chunk;
                        this.startDate = date;
                    }
                }
            }
            this.lastData = chunk;
            this.lastDate = date;
        }
        callback();
    }
    // Given the date and data for an end of a period, forms a descr of the 
    // period using the recorded start.
    getData( date, data ) {
        var rawStart = this.startData['Open'];
        var rawEnd = data['Close'];
        var adjEnd = data['Adj Close'];
        var adjStart = (rawStart * adjEnd) / rawEnd;
        return {
            start: { date:     this.startDate,
                     price:    rawStart,
                     adjPrice: adjStart },
            end:   { date:     date,
                     price:    rawEnd,
                     adjPrice: adjEnd } 
        };
    }
}

async function* genForStream(strm) {
  for await (const chunk of strm) {
    yield chunk;
  }
}

// Returns an async generator with rows from the CSV file at the given path.
async function* genForCSV(csvPath) {
  const strm = genForStream( fs.createReadStream(csvPath).pipe(csv()) );
  for await (const chunk of strm) {
    yield chunk;
  }
}

// Defines a source for stock data.  Fields are:
//  ticker: The ticker named.  used to form csv file in dataDir.  Required
//  start:  Include dates on or after.  Defaults to first in CSV
//  end:    Exclude dates after.  Defaults to last in CSV
//  freq:   daily, weekly, or monthly.  Defaults to daily.
class StockSource 
{
    constructor( values ) {
        Object.assign( this, values );
    }
}

// If values is empty, await a new one from the stream, and put it into values.
// Return values[0] without shifting.
// Return undefined if values exhausted.
async function getInput( strm, values )
{
  try {
    if (values.length > 0) {
        return values[0];
    }
    var pull = await strm.next();
    if (pull.done) {
        return undefined;
    }
    values.push( pull.value );
    return values[0];
  } catch (e) {
    console.log("ERROR: reading input: "+e );
  }
}

// An async function that gets one or two intervals from a stream, aligned
// with the start and end dates of 'primary'.  
// 'values' is a queue that may be read and written.  Looks sequentially
// for values, first pulling values from the queue, and when its empty,
// awaiting them from the stream.  If a value is read from the stream, but
// is after the end of the interval, it is added to the queue.  If the stream
// is empty, or if the interval was not matched, return undefined.
//
async function nextRefVal( targetVal, strm, values ) {
    // Get the start value.
    var start = undefined;
    while (start == undefined) {
        var val;
        try {
            val = await getInput( strm, values );
        } catch (e) {
            console.log("ERROR: reading input: "+e);
            return undefined;
        }
        if (val == undefined) {
            return undefined;
        }
        if (val.end.date < targetVal.start.date) {
            // Ends before target start: discard.
            values.shift();
        } else if (val.start.date > targetVal.end.date) {
            // The stream is ahead of the target: no values yet.
            return undefined;
        } else {
            values.shift;
            start = val;
        }
    }

    // Get the end value.
    if (start.end.date >= targetVal.end.date) {
        // Exact match, or start goes over end.  start==end.
        return {start: start, end: start};
    }

    // Get the end value.
    var end = undefined;
    var last = start;
    while (true) {
        var val = getInput( strm, values );
        if ((val == undefined) || (val.start.date > targetVal.end.date)) {
            // adopt last read as end.
            return {start: start.start, end: last.end};
        } else if (val.end.date >= targetVal.end.date) {
            // Matches or overlaps.
            return {start: start.start, end: val.end};
        } else {
            last = values.shift();
        }
    }

    return undefined;
}

// Accepts a StockSource, and returns an async generator
// returning {start,end} pairs for each interval.
//
// If the optional refStocks is given, it is an array of StockSource's
// That are streamed in parallel. Values from refStocks are included
// in each result if there is data with matching start and end dates.
// This is done in 2 ways:
// 1. If a ref interval's start and end match the main interval, the
//    data from the ref interval is included.
// 2. If the ref interval is daily, and the main is not, then ref interval
//.   data is included from a pair of ref intervals matching start and end,
//    and intervening ref intervals are dropped.
// This allows ref streams to compute metrics on a daily basis to be included
// in a more granular stream.
async function* genIntervalsForStock( stock, refStocks ) {
    if (stock.ticker == undefined) {
        throw "No ticker defined for stock."
    }
    var path = csvPath(stock.ticker);
    if (!fs.existsSync(path)) {
        throw "CSV file \'"+path+"\' does not exist.";
    }
    const primStrm = genForStream( fs.createReadStream(path)
                                 .pipe(csv())
                                 .pipe( new StockIntervals(stock)));
    var refStrms = [];
    var refValues = []; // Array of quueues
    if (refStocks != undefined) {
        refStocks.forEach( function (refStock) {
            var refPath = csvPath(refStock.ticker);
            if (!fs.existsSync(refPath)) {
                throw "CSV file \'"+refPath+"\' does not exist.";
            }
            refStrms.push(genForStream( fs.createReadStream(refPath)
                                     .pipe(csv())
                                     .pipe( new StockIntervals( refStock ))));
            refValues.push([]);
        });
    }

    // Generate a value for each primary stream value.
    for await (const primary of primStrm) {
        primary.ticker = stock.ticker;
        if (refStrms.length == 0) {
            // If there are no refStocks, just yield the primary value.
            yield primary;
        } else {
            // Get values for the start and end time from each ref stream.
            var rslt = { primary: primary, refs: [] };
            for ( var iref = 0; iref < refStocks.length; iref++ ) {
                var val = await nextRefVal( primary, refStrms[iref], refValues[iref] );
                val.ticker = refStocks[iref];
                rslt.refs.push( val );
            }
            yield rslt;
        }
    }
}

module.exports = {
    setDataDir: setDataDir,
    genForStream: genForStream,
    genForCSV: genForCSV,
    genIntervalsForStock: genIntervalsForStock,
    StockSource: StockSource
}
