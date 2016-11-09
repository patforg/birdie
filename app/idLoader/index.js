var config = require('../../config.json');
var fs = require('fs');
var util = require('util');
var stream = require('stream');
var es = require('event-stream');

var idCount = 0;
var ids = [];
var idCallback;
var reachedEnd = false;
var offsetId = null;
var lineCount = 0;
var stream = null;

/**
 * oepen stream and checks finds the ids were last importing
 * then push a bunch of ids to be process when we have enough
 * ids to do an API call 
 */
var setupStream = function () {
    stream = fs.createReadStream(config.input_file)
        .pipe(es.split())
        .pipe(es.mapSync(function(line){
            lineCount++;
            if (line.length > 0) {
                if (offsetId !== null) {
                    if (offsetId == line) {
                        console.log('Skipped '+ lineCount +' lines.');

                        offsetId = null;
                    } //if
                } else {
                    idCount++;
                    ids.push(line);
                }
            } else {
                console.log('empty line');
            } //if

            if (idCount >= config.tweets_per_call) {
                // pause the readstream
                stream.pause();
                pushIds();
            } //if
        })
        .on('error', function(err){
            reachedEnd = true;
            if (idCallback) idCallback(err);
        })
        .on('end', function(){
            console.log('Reached end of list, read '+ lineCount +' lines');
            reachedEnd = true;
            pushIds();
        })
    );
};

/**
 * take the numbers of ids needed to make
 * and API call and copy it to another array
 * then send it to the processor function
 * that was passed when the request for ids
 * was made
 */
var pushIds = function () {
    var idsCopy;
    if (idCallback) {
        idsCopy = ids.splice(0, config.tweets_per_call);
        idCount = ids.length;
        idCallback(null,idsCopy);
    } //if
};

/**
 * function that will fetch the next
 * ids and call the processor function 
 * when the ids have been read
 */
var next = function(callback) {
    if (reachedEnd) {
        idCallback = null;
        callback(null, []);
        return;
    } //if

    idCallback = callback;
    if (stream !== null) {
        stream.resume();
    } else {
        setupStream();
    } //if
};

/**
 *  id of last saved tweet
 *  will scan file until this 
 *  id is found and continue importing
 *  from there
 */
var setOffset = function (offset) {
    if (offset) {
        offsetId = offset;
    } //if
};

module.exports = function () {
    return { 
        setOffset: setOffset,
        next: next
    };
};
