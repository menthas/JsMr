var Sequelize = require('sequelize');
var fs = require('fs');
var path = require('path');

module.exports = function (conf) {
    if (conf == undefined && module.parent.exports.conf)
        conf = module.parent.exports.conf;

    /**
     * Log a message to console if debug mode is on
     * @param  {String} msg   The message to log
     * @param  {String} level The log level
     */
    this.log = function (msg, level) {
        if (conf && !conf.get('debug'))
            return;
        if (level == undefined)
            level = "info";
        console.log(level.toUpperCase() + ": " + msg);
    }

    /**
     * Converts a number in seconds to the format: 5h 25m 17s
     * @param  {Integer} seconds number in seconds
     * @return {String}          xh ym zs
     */
    this.time_string = function(seconds) {
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

    this.cleanup = function(storage) {
        var chain = new Sequelize.Utils.QueryChainer();

        // Cleanup jobs that are not in the db.
        var job_dir = path.normalize(__dirname+'/../jobs/');
        var job_files = fs.readdirSync(job_dir);
        for (var i=0; i<job_files.length; i++) {
            var job_id = path.basename(job_files[i], ".js");
            if (job_id != "_schema")
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
                job_id = path.basename(job_files[j], ".js");
                if (job_id != "_schema" && existing_jobs.indexOf(job_id) == -1) {
                    fs.unlink(job_dir + "/" + job_files[j]);
                }
            }
        });
    }

    // so that we can call methods from the caller.
    return this;
}
