var sqlite3 = require('sqlite3').verbose();
var async = require('async');
var config = require('../../config.json');
var db = new sqlite3.Database(config.db_file);

var saveTweetStatement;
var saveUserStatement;

/** Returns configured db connection */
var getDB = function () { 
    return db; 
};

/** Creates required tables */
var createTables = function (done) {
    async.parallel([
        function (next) {
            db.run("CREATE TABLE IF NOT EXISTS tweets (\
                `id` TEXT PRIMARY KEY NOT NULL, \
                `text` TEXT NOT NULL, \
                `created_at` TEXT NOT NULL, \
                `truncated` INTEGER NOT NULL DEFAULT 0, \
                `source` TEXT NOT NULL, \
                `in_reply_to_status_id` TEXT NOT NULL, \
                `in_reply_to_user_id` TEXT NOT NULL, \
                `in_reply_to_screen_name` TEXT NOT NULL, \
                `user_id` TEXT NOT NULL, \
                `longitude` REAL, \
                `latitude` REAL, \
                `is_quote` INTEGER NOT NULL DEFAULT 0, \
                `retweet_count` INTEGER NOT NULL DEFAULT 0, \
                `favorite_count` INTEGER NOT NULL DEFAULT 0, \
                `lang` TEXT NOT NULL \
            );", next);
        },

        function (next) {

            db.run("CREATE TABLE IF NOT EXISTS users (\
                `id` TEXT PRIMARY KEY NOT NULL, \
                `name` TEXT NOT NULL, \
                `screen_name` TEXT NOT NULL, \
                `location` TEXT NOT NULL, \
                `description` TEXT NOT NULL, \
                `url` TEXT NOT NULL, \
                `followers_count` INTEGER NOT NULL DEFAULT 0, \
                `friends_count` INTEGER NOT NULL DEFAULT 0, \
                `listed_count` INTEGER NOT NULL DEFAULT 0, \
                `created_at` TEXT NOT NULL, \
                `favorites_count` INTEGER NOT NULL DEFAULT 0, \
                `utc_offset` INTEGER NOT NULL DEFAULT 0, \
                `time_zone` TEXT NOT NULL, \
                `geo_enabled` INTEGER NOT NULL DEFAULT 0, \
                `verified` INTEGER NOT NULL DEFAULT 0, \
                `statuses_count` INTEGER NOT NULL DEFAULT 0, \
                `lang` TEXT NOT NULL, \
                `contributors_enabled` INTEGER NOT NULL DEFAULT 0, \
                `is_translator` INTEGER NOT NULL DEFAULT 0, \
                `is_translaton_enabled` INTEGER NOT NULL DEFAULT 0, \
                `profile_image_url` TEXT NOT NULL, \
                `profile_image_url_https` TEXT NOT NULL, \
                `default_profile` INTEGER NOT NULL DEFAULT 0, \
                `default_profile_image` INTEGER NOT NULL DEFAULT 0, \
                `notifications` INTEGER NOT NULL DEFAULT 0, \
                `translator_type` TEXT NOT NULL \
            );", next);

        }], done); 
};

/** Prepares save queries */
var prepareStatements = function (done) {
    async.parallel([
        function (next) {
            saveTweetStatement = db.prepare("INSERT OR IGNORE INTO `tweets` \
            ( \
                id, text, created_at, \
                truncated, source, in_reply_to_status_id,\
                in_reply_to_user_id, in_reply_to_screen_name, user_id, \
                longitude, latitude, is_quote, \
                retweet_count, favorite_count, lang \
            \
            ) VALUES(\
                $id, $text, $created_at, \
                $truncated, $source, $in_reply_to_status_id,\
                $in_reply_to_user_id, $in_reply_to_screen_name, $user_id, \
                $longitude, $latitude, $is_quote, \
                $retweet_count, $favorite_count, $lang \
            )", next);
        },

        function (next) {
            saveUserStatement = db.prepare("INSERT OR IGNORE INTO `users` \
            ( \
                id, name, screen_name, \
                location, description, url, \
                followers_count, friends_count, listed_count, \
                created_at, favorites_count, utc_offset, \
                time_zone, geo_enabled, verified, \
                statuses_count, lang, contributors_enabled, \
                is_translator, is_translaton_enabled, profile_image_url, \
                profile_image_url_https, default_profile, default_profile_image, \
                notifications, translator_type \
            ) VALUES (\
                $id, $name, $screen_name, \
                $location, $description, $url, \
                $followers_count, $friends_count, $listed_count, \
                $created_at, $favorites_count, $utc_offset, \
                $time_zone, $geo_enabled, $verified, \
                $statuses_count, $lang, $contributors_enabled, \
                $is_translator, $is_translaton_enabled, $profile_image_url, \
                $profile_image_url_https, $default_profile, $default_profile_image, \
                $notifications, $translator_type \
            )", next);
        },

   ], done); 
};

/**
 * Setup the db and queries to start saving data
 */
var init = function (done) { 
    var lastId = null;
    async.series([
        createTables,
        prepareStatements,
        function (next) {
            db.get("SELECT id FROM tweets ORDER BY rowid DESC LIMIT 1", function (err, row) {
                if (err) next(err);

                if (row) {
                    lastId = row.id;
                } //if
                next();
            });
        }
    ], function (err) {
        done(err, lastId);
    });
};

/**
 * finalizes existing statements and closes DB connection
 */
var close = function (done) {
    async.parallel([
        function (next) {
            if (saveTweetStatement) {
                saveTweetStatement.finalize(next);
            } else {
                next();
            } //if
        },

        function (next) {
            if (saveUserStatement) {
                saveUserStatement.finalize(next);
            } else {
                next();
            } //if
        }
    ], function () {
        db.close();
        if (done) done();
    });
};

var saveCount = 0;
/** Returns number of saved tweets in this session */
var getSaveCount = function () {
    return saveCount;
};

/**
 * Save tweet and associated user data
 */
var saveTweet = function(tweet, done) {
    async.parallel([
        function (next) {
            saveCount++;
            saveTweetStatement.run({
                $id: tweet['id_str'],
                $text: tweet['text'],
                $created_at: tweet['created_at'],
                $truncated: tweet['truncated'] ? 1 : 0,
                $source: tweet['source'],
                $in_reply_to_status_id: tweet['in_reply_to_status_id'] || '' ,
                $in_reply_to_user_id: tweet['in_reply_to_user_id'] || '',
                $in_reply_to_screen_name: tweet['in_reply_to_screen_name'] || '',
                $user_id: tweet.user['id_str'],
                $longitude: tweet['coordinates'] ? tweet['coordinates']['coordinates'][0] : null,
                $latitude: tweet['coordinates'] ? tweet['coordinates']['coordinates'][1] : null,
                $is_quote: tweet['is_quote'] ? 1 : 0,
                $retweet_count: tweet['retweet_count'],
                $favorite_count: tweet['favorite_count'],
                $lang: tweet['lang']
            }, next);
        },
        function (next) {
            saveUserStatement.run({
                $id: tweet.user['id_str'],
                $name: tweet.user['name'],
                $screen_name: tweet.user['screen_name'],
                $location: tweet.user['location'],
                $description: tweet.user['description'],
                $url: tweet.user['url'],
                $followers_count: tweet.user['followers_count'],
                $friends_count: tweet.user['friends_count'],
                $listed_count: tweet.user['listed_count'],
                $created_at: tweet.user['created_at'],
                $favorites_count: tweet.user['favorites_count'],
                $utc_offset: tweet.user['utc_offset'],
                $time_zone: tweet.user['time_zone'],
                $geo_enabled: tweet.user['geo_enabled'] ? 1 : 0,
                $verified: tweet.user['verified'] ? 1 : 0,
                $statuses_count: tweet.user['statuses_count'],
                $lang: tweet.user['lang'],
                $contributors_enabled: tweet.user['contributors_enabled'] ? 1 : 0,
                $is_translator: tweet.user['is_translator'] ? 1 : 0,
                $is_translaton_enabled: tweet.user['is_translaton_enabled'] ? 1 : 0,
                $profile_image_url: tweet.user['profile_image_url'],
                $profile_image_url_https: tweet.user['profile_image_url_https'],
                $default_profile: tweet.user['default_profile'] ? 1 : 0,
                $default_profile_image: tweet.user['default_profile_image'] ? 1 : 0,
                $notifications: tweet.user['notifications'] ? 1 : 0,
                $translator_type: tweet.user['translator_type']
            }, next);
        }
    ], done);
};

module.exports = function() {
    return {
        getDB: getDB,
        init: init,
        close: close,
        saveTweet: saveTweet,
        getSaveCount: getSaveCount
    };
};
