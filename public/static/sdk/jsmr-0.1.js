

/* Simple JavaScript Inheritance
 * By John Resig http://ejohn.org/
 * MIT Licensed.
 */
// Inspired by base2 and Prototype
(function(){
    var initializing = false, fnTest = /xyz/.test(function(){xyz;}) ? /\b_super\b/ : /.*/;
 
    // The base Class implementation (does nothing)
    this.Class = function(){};
 
    // Create a new Class that inherits from this class
    Class.extend = function(prop) {
        var _super = this.prototype;
     
        // Instantiate a base class (but only create the instance,
        // don't run the init constructor)
        initializing = true;
        var prototype = new this();
        initializing = false;
     
        // Copy the properties over onto the new prototype
        for (var name in prop) {
            // Check if we're overwriting an existing function
            prototype[name] = typeof prop[name] == "function" &&
                typeof _super[name] == "function" && fnTest.test(prop[name]) ?
                (function(name, fn){
                    return function() {
                        var tmp = this._super;
                     
                        // Add a new ._super() method that is the same method
                        // but on the super-class
                        this._super = _super[name];
                     
                        // The method only need to be bound temporarily, so we
                        // remove it when we're done executing
                        var ret = fn.apply(this, arguments);
                        this._super = tmp;
                     
                        return ret;
                    };
                })(name, prop[name]) :
                prop[name];
        }
     
        // The dummy class constructor
        function Class() {
            // All construction is actually done in the init method
            if ( !initializing && this.init )
                this.init.apply(this, arguments);
        }
     
        // Populate our constructed prototype object
        Class.prototype = prototype;
     
        // Enforce the constructor to be what we expect
        Class.prototype.constructor = Class;
 
        // And make this class extendable
        Class.extend = arguments.callee;
     
        return Class;
    };
})();

var JsMr = Class.extend({
    init: function () {
        this.registered = false;
        this.client_id = null;
        this.last_call = 0;
        this.current_task = null;
        this.beat_interval_obj = null;
        this.options = {
            beat_interval: 60000, // one minute
            auth_token: null,
            server_path: '',
            debug: true,
            jquery_cdn: 'https://ajax.googleapis.com/ajax/libs/jquery/2.1.3/jquery.min.js',
        }

        // Start the SDK
        this.loadAndRegister();
    },

    /**
     * Register a client with the server
     * @param  {Object} self A reference to the SDK object since register is
     *                  called from nested functions and reference to `this` is
     *                  lost
     */
    register: function (self) {
        jQuery.post(
            self.url('register'),
            {
                auth_token: self.options.auth_token,
                action: 'register',
                agent: navigator.userAgent,
            },
            function(data) {
                if (data.registered == true) {
                    self.registered = true;
                    self.client_id = data.client_id;
                    self.runTask(data.task);
                    self.beat_interval_obj = setInterval(function () {
                        self.beat();
                    }, self.options.beat_interval);
                    self.log("Client Registered with id " + data.client_id);
                } else {
                    self.log("Server refused the registration request.");
                }             
            }, 'json'
        ).fail(function () {
            self.log("Failed to contact server at " + self.url('register'), true);
        });
        self.serverCalled();
    },

    /**
     * Unregister the client from the server and stop any activity by the SDK
     */
    unregister: function() {
        if (!this.registered)
            return;
        var _this = this;
        jQuery.post(
            this.url('register'),
            {
                auth_token: this.options.auth_token,
                client_id: this.client_id,
                action: 'unregister',
            },
            function (data) {
                // TODO maybe stop the running task ?
                if (data.unregistered == true) {
                    _this.registered = false;
                    _this.client_id = null;
                    clearInterval(_this.beat_interval_obj);
                    _this.log("Client removed from server pool.");
                } else {
                    _this.log("Failed to unregister client from server.", true);
                }
            }
        );
    },

    /**
     * Run a new task, the client must be registered at this point. Also any
     * running tasks will be stopped.
     * @param  {Object} task The task info
     */
    runTask: function (task) {
        if (task == null)
            return;
        if (!this.registered) {
            this.log("Can't run Task `"+task.task_id+"`, client not registered", true);
        }
        if (this.current_task != null) {
            // stop and cleanup the current task before starting a new one
        }

        // TODO run the task !
    },

    /**
     * Send a heartbeat to the server to avoid getting removed from the pool.
     * The hearbeat will only be sent if there has been no communication with
     * the server for the past `beat_interval` milliseconds.
     */
    beat: function () {
        // Don't send beat if we've communicated with the server recently
        if (jQuery.now() - this.last_call < this.options.beat_interval)
            return;
        var _this = this;
        jQuery.get(
            this.url('beat'),
            {
                auth_token: this.options.auth_token,
                client_id: this.client_id,
                task_id: this.current_task ? this.current_task.task_id : null,
                job_id: this.current_task ? this.current_task.job_id : null,
            },
            function(data) {
                if (data.valid != true) {
                    _this.runTask(data.new_task);
                }
                _this.log("Heartbeat ❤")
            }, 'json'
        ).fail(function () {
            _this.log("Failed to sent heartbeat to " + _this.url('beat'), true);
        });
    },

    /**
     * This SDK relies on jQuery for AJAX calls. This method will load jQuery
     * only if not it's not already in the global scope and will register the
     * client with the server.
     */
    loadAndRegister: function() {
        var _this = this;
        if (typeof jQuery == 'undefined') {
            var getScript = function(url, success) {
                var script = document.createElement('script');
                script.src = url;
                var head = document.getElementsByTagName('head')[0],
                    done = false;
                // Attach handlers for all browsers
                script.onload = script.onreadystatechange = function() {
                    if (!done && (!this.readyState || this.readyState == 'loaded' || this.readyState == 'complete')) {
                        done = true;
                        // callback function provided as param
                        success(_this);
                        script.onload = script.onreadystatechange = null;
                        head.removeChild(script);
                    };
                };
                head.appendChild(script);
            };
            getScript(this.options.jquery_cdn, this.register);
        } else { // jQuery was already loaded
            this.register();
        };
    },

    /**
     * Create a API url by prepending the server path.
     * @param  {String} path API call path
     * @return {String}      Full API path.
     */
    url: function(path) {
        return this.options.server_path + '/' + path;
    },

    /**
     * Log a message to console if debug mode is on
     * @param  {String}  message Message to log
     * @param  {Boolean} error   Is this an error or not.
     */
    log: function(message, error) {
        if (this.options.debug) {
            if (error == true)
                console.error("JsMr: " + message);
            else
                console.log("JsMr: " + message);
        }
    },

    /**
     * Updates the last activity of this client. used to avoid making extra
     * API calls.
     */
    serverCalled: function() {
        this.last_call = jQuery.now();
    }
});
