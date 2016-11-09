/**
 * Module to configure the Twitter client
 */
var Twitter = require('twitter');
var config = require('../../config.json');
var client = new Twitter(config.twitter_api);

module.exports = function () {
    return client;
};
