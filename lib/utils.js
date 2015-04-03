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

    // so that we can call methods from the caller.
    return this;
}
