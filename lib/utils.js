module.exports = function (conf) {
    if (conf == undefined && module.parent.exports.conf)
        conf = module.parent.exports.conf;
    else if (conf == undefined)
        throw "Configuration is required to load module";

    /**
     * Log a message to console if debug mode is on
     * @param  {String} msg   The message to log
     * @param  {String} level The log level
     */
    this.log = function (msg, level) {
        if (!conf.get('debug'))
            return;
        if (level == undefined)
            level = "info";
        console.log(level.toUpperCase() + ": " + msg);
    }

    // so that we can call methods from the caller.
    return this;
}
