var Sequelize = require('sequelize');
var fs = require('fs');
var path = require('path');
var conf = module.parent.exports.conf

/**
 * Available log levels, can be used to limit the log verbosity
 * @type {Object}
 */
module.exports.ll = {
    FATAL: 0,
    ERROR: 1,
    WARNING: 2,
    NOTICE: 3,
    INFO: 4,

    // log level names, for reverse looksup
    0: 'FATAL', 1: 'ERROR', 2: 'WARNING', 3: 'NOTICE', 4: 'INFO'
};

/**
 * Log a message to console if debug mode is on
 * @param  {String} msg   The message to log
 * @param  {String} level The log level
 */
module.exports.log = function (msg, level) {
    if (!conf.get('debug') || level > conf.get("log_level"))
        return;
    if (level == undefined)
        level = 4; // default to INFO

    var d = new Date();
    var time = d.getDay() + "-" + d.getMonth() + "-" + d.getFullYear() +
        " " + d.getHours() + ":" + d.getMinutes() + ":" + d.getSeconds();
    console.log(time + " [" + module.exports.ll[level] + "] " + msg);
}

/**
 * Converts a number in seconds to the format: 5h 25m 17s
 * @param  {Integer} seconds number in seconds
 * @return {String}          xh ym zs
 */
module.exports.time_string = function(seconds) {
    var str = "";
    if (seconds > 3600) {
        var hours = Math.floor(seconds / 3600);
        seconds %= 3600;
        str += hours + "h ";
    }
    if (seconds > 60) {
        var mins = Math.floor(seconds / 60);
        seconds %= 60;
        str += mins + "m ";
    }

    return str + seconds + "s";
}

module.exports.size_string = function(bytes) {
    if (bytes > 1000000)
        return (bytes / 1000000).toFixed(3) + "MB";
    else if (bytes > 1000)
        return (bytes / 1000).toFixed(1) + "KB";
    return bytes + "B";
}

/**
 * Cleanup the server's state after a crash or imperfect startup
 * @param  {Object} storage holding the DB and ORM
 */
module.exports.cleanup = function(storage) {
    var chain = new Sequelize.Utils.QueryChainer();

    // Cleanup jobs that are not in the db.
    var job_dir = path.normalize(__dirname+'/../jobs/');
    var job_files = fs.readdirSync(job_dir);
    for (var i=0; i<job_files.length; i++) {
        var job_id = path.basename(job_files[i], ".job");
        // job files starting with '_' are sample jobs and shouldn't be removed.
        if (job_id[0] != '_')
            chain.add(storage.Job.find(job_id));
    }
    chain.run().success(function (result) {
        var existing_jobs = [];
        var job_id;
        for (var j=0; j<result.length; j++) {
            if (result[j] != null)
                existing_jobs.push(result[j].id);
        }
        for (var j=0; j<job_files.length; j++) {
            job_id = path.basename(job_files[j], ".job");
            if (job_id[0] != '_' && existing_jobs.indexOf(job_id) == -1) {
                fs.unlink(job_dir + "/" + job_files[j]);
            }
        }
    });
}
