var ps = require("./modules/price_stream");
ps.setDataDir("./data");

var SSO = new ps.StockSource(
    {ticker: "SSO", 
     start: "2011-01-01", 
     end: undefined, 
     freq: "monthly"
    } );

var SPY = new ps.StockSource(
    {ticker: "SPY", 
     start: undefined,
     end: undefined, 
     freq: "monthly"
    } );


// A view of the current market period, with the option to keep a history 
// of several preceding periods
class Market {
    constructor( values ) {
        Object.assign( this, {
                        periods: [], // Up to keep_periods last periods from stream.
                        keep_periods: 1
                       }, values )
        }

    // Pops off old periods, and places the new one at [0].
    newPeriod( period ) {
        while (this.periods.length >= this.keep_periods) {
            this.periods.pop();
        }
        this.periods.unshift(period)
        return period;
    }
    current() {
        if (this.periods.length > 0) {
            return this.periods[0];
        } else {
            return undefined;
        }
    }
    // Finds the data entry for the given ticker, at the given depth.
    // if no ticker is given, gets the primary.
    // If no depth is given, gets the current.
    findData( ticker, depth ) {
        var period;
        if ((depth == undefined) || (depth >= periods.length) || (depth < 0)) {
            period = this.current();
        } else {
            period = periods[depth];
        }
        if ((ticker == undefined) || (period.primary.ticker == ticker)) {
            return period.primary;
        }
        period.refs.forEach( function(ref) {
            if (ref.ticker == ticker) {
                return ref;
            }
        });
        return period.primary;
    }
    startDate( ticker, depth ) {
        var data = this.findData( ticker, depth );
        return data.start.date;
    }
    endDate( ticker, depth ) {
        var data = this.findData( ticker, depth );
        return data.end.date;
    }
    startPrice( ticker, depth ) {
        var data = this.findData( ticker, depth );
        return data.start.price;
    }
    endPrice( ticker, depth ) {
        var data = this.current();
        return data.start.price;
    }
    startAdjPrice( ticker, depth ) {
        var data = this.findData( ticker, depth );
        return data.start.adjPrice;
    }
    endAdjPrice( ticker, depth ) {
        var data = this.findData( ticker, depth );
        return data.start.adjPrice;
    }

}

class Position {
    constructor( values ) {
        Object.assign( this, {
                         ticker: 'none',// can be cash
                         shares: 0,
                         basis:  0.0,
                         acquired:  undefined, // Date
                         value:  0.0,   //in some context
                         kind:   "long" //long or short
                       }, values );
    }
}

class Action {
    constructor (kind, item, num, price) {
        Object.assign( this, {
                        kind: kind,
                        item: item,
                        num:  num,
                        price: price
                       });
    }
}

// A view of an account at a point in time.
class AccountSnapshot {
    constructor( values ) {
        Object.assign( this, {
                        positions: [],  // Indexed by ticker, contains array of Positions
                        date: undefined
                        }, values );
    }

    syncToMarket( market ) {
        // Updates the date to the end date of the current market period.
        this.date = market.endDate();
    }

    // Returns a ref to the current positions for the given ticker.
    positionsFor( ticker ) {
    }

    // Add or remove cash.
    deposit( value ) {
        var newSS = new AccountSnapshot( this );
        return {action: new Action("deposit","cash",value, 1.0), ss: newSS};
    }
    withdraw( value ) {
        var newSS = new AccountSnapshot( this );
        return {action: new Action("withdraw","cash",value, 1.0), ss: newSS};
    }

    
    reprice( market) {
        var newSS = new AccountSnapshot( this );
		// No positions currently. Need to add positions, and change its price.
        return {action: new Action("price","",0,market.endAdjPrice()), ss: newSS};
                
    }
    buy( market, ticker, shares ) {
        var newSS = new AccountSnapshot( this );
        return {action: new Action("buy",ticker,shares, market.endAdjPrice()), ss: newSS};
                
    }
    sell( market, ticker, shares ) {
        var newSS = new AccountSnapshot( this );
        return {action: new Action("sell",ticker,shares, market.endAdjPrice()), ss: newSS};
    }
    takeShort( market, ticker, shares ) {
        var newSS = new AccountSnapshot( this );
        return {action: new Action("short",ticker,shares, market.endAdjPrice()), ss: newSS};
    }
    coverShort( market, ticker, shares ) {
        var newSS = new AccountSnapshot( this );
        return {action: new Action("cover",ticker,shares, market.endAdjPrice()), ss: newSS};
    }

    // Return {total, cash, stock, short}
    currentValue() {
    }
}

class Strategy {
    constructor( values ) {
        Object.assign( this, {
                        }, values );
    }

    trade( market, account) {
        return [];
    }
}

class Simulator {
    constructor( values ) {
        Object.assign( this, {
                        market: undefined,
                        account: undefined,
                        strategy: undefined
                        }, values );
    }
    // Async generator that scans a sequence of periods and 
    // returns a sequence of arrays of transactions.
    async* run( periods ) {
        for await (const period of periods)  {
            this.market.newPeriod( period );
            this.account.syncToMarket( this.market );
            var sync = [this.account.reprice( this.market )]; // Returns "price" transactions.  'ss' field is a record of current account.
            var trades = this.strategy.trade( this.market, this.account );  // Returns trade transactions.
            yield sync.concat(trades); 
        }
    }
}

async function main() {

    var sim = new Simulator();
    sim.account = new AccountSnapshot();
    sim.account.deposit( 100000 );

    sim.market = new Market();
    sim.strategy = new Strategy();
        
    var prices = ps.genIntervalsForStock(SSO,[SPY]);
    for await (const txs of sim.run( prices )) {
        txs.forEach( function (tx) {
            console.log( JSON.stringify( tx ) );
        });
    }
}


main()
