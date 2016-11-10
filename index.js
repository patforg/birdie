var config = require('./config.json');
var dbUtil = require('./app/dbUtil/')();
var twitterClient = require('./app/twitter/')();
var idLoader = require('./app/idLoader/')();
var async = require('async');

var maxIterations = config.max_iterations;
var callsLeft = config.limit_per_15_minutes;

/* Reset API limit every 15 minutes */
var rateLimitInterval = setInterval(function () {
    callsLeft = config.limit_per_15_minutes;
    importNextBatch();
}, (1000 * 60 * 15));

/**
 * Tries to import next batch of tweets
 * making sure we respect the API 
 * rate limit
 */
var importNextBatch = function () {
    /* check if we attained the user set limit */
    if (maxIterations !== 0) {
        
        /* check if we attained our rate limit */
        if (callsLeft > 0) {
            idLoader.next(processIds);
        } else {
            process.stdout.write("\n");
            console.log('Saved '+ dbUtil.getSaveCount() +' so far in this session.');
            console.log('API rate limit reached, import will resume when it is safe to do so (less than 15 mins).');
        } //if

    } else {
        finish();
    } //if
};

/**
 * Process a bunch of tweet ids once they are read 
 * from the input file
 */
var processIds = function(err, ids) {
    if (err) throw err;

    if (ids.length > 0) {
        maxIterations--;
        callsLeft--;
        var params = {
            id: ids.join(","),
            map: false
        };
    
        twitterClient.get("statuses/lookup", params, function (err, tweets, response) {
            if (err) {
                console.log(err);
                importNextBatch();
                return;
            } //if

            process.stdout.write('.');
            if (tweets.length == 0) {
                importNextBatch();
            } else {
                async.eachSeries(tweets, dbUtil.saveTweet, function (err) {
                    if (err) throw err;
                    importNextBatch();
                });
            } //if
        });
   
    } else {
        finish();
    } //if
};

/**
 * does database and timer cleanup
 */
var finish = function () {
    process.stdout.write("\n");
    console.log('Cleaning up');
    clearInterval(rateLimitInterval);
    dbUtil.close();
};

/**
 * Configure DB and launch import
 */
dbUtil.init(function (err, lastId) {
    if (err) throw err;
    if (lastId && lastId.length > 0) {
        idLoader.setOffset(lastId);
    } //if
    importNextBatch();
});
