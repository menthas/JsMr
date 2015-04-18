/* Simple JavaScript Inheritance
 * By John Resig http://ejohn.org/
 * MIT Licensed.
 */
// Inspired by base2 and Prototype
(function () {
    var initializing = false, fnTest = /xyz/.test(function () {
        xyz;
    }) ? /\b_super\b/ : /.*/;

    // The base Class implementation (does nothing)
    this.Class = function () {
    };

    // Create a new Class that inherits from this class
    Class.extend = function (prop) {
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
                (function (name, fn) {
                    return function () {
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
            if (!initializing && this.init)
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
        this.current_output = "";
        this.current_state = {};
        this.step_list = [];

        this.beat_interval_obj = null;
        this.aws_sdk = 'https://sdk.amazonaws.com/js/aws-sdk-2.1.21.min.js';
        this.s3 = null;
        this.options = {
            beat_interval: 60000, // one minute
            auth_token: null,
            server_path: '',
            debug: true,
            jquery_cdn: 'https://ajax.googleapis.com/ajax/libs/jquery/2.1.3/jquery.min.js'
        };
        this.requirements = {
            jQuery: false,
            AWS: false
        }
        this.loadAndRegister();
    },

    /**
     * Upload the output and state to AWS S3
     *
     */
    uploadData: function () {
        var _this = this;
        var task = this.current_task;
        var job_id = task.job_id.toString();
        var task_id = task.task_id.toString();
        var state = JSON.stringify(this.current_state);

        this.log('Output is' + this.current_output);
        this.log('State is' + state);

        var output_params = {
            Bucket: task.bucket_name, /* required */
            Key: job_id.concat('_', task.task_id),
            Body: this.current_output
        };

        var state_params = {
            Bucket: task.bucket_name, /* required */
            Key: job_id.concat('_', task.step, '_', task.instance_id),
            Body: state
        };


        this.s3.upload(output_params, function (err, data) {
            if (err)
                _this.log(err, err.stack); // an error occurred
            else {
                _this.s3.upload(state_params, function (err, data) {
                    if (err)
                        _this.log(err, err.stack); // an error occurred
                    else {
                        _this.updateTast(_this.current_task, "task_success");
                    }
                });
            }
        });


    },


    register: function (self) {
        all_loaded = true;
        for (lib in self.requirements)
            all_loaded = all_loaded && self.requirements[lib];
        if (!all_loaded)
            return;
        jQuery.post(
            self.url('register'),
            {
                auth_token: self.options.auth_token,
                action: 'register',
                agent: navigator.userAgent
            },
            function (data) {
                var action = 'task_success'
                if (data.registered == true) {
                    self.registered = true;
                    self.client_id = data.client_id;
                    try {
                        self.runTask(data.task);
                    } catch (err) {
                        self.log("Error occured during running the task. " + err);
                        action = 'task_failure'
                        self.updateTast(data.task, action);
                    }
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
    unregister: function () {
        if (!this.registered)
            return;
        var _this = this;
        jQuery.post(
            this.url('register'),
            {
                auth_token: this.options.auth_token,
                client_id: this.client_id,
                action: 'unregister'
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
     * Update task in case of failure of tasks,
     * task success.
     */
    updateTast: function (task, action) {
        var _this = this;
        jQuery.post(
            _this.url('task'),
            {
                task_id: task.task_id,
                action: action
            },
            function (data) {
                // TODO
            }
        ).fail(function () {
                _this.log("Failed to update the task at " + _this.url('task'), true);
            });
    },


    getData: function (self) {
        var task = self.current_task;
        var credentials = {accessKeyId: task.access_key, secretAccessKey: task.secret_key};

        AWS.config.update(credentials);
        AWS.config.region = task.aws_region;
        self.s3 = new AWS.S3();

        //Fetch state from AWS
        var job_id = task.job_id.toString();
        var state_params = {
            Bucket: task.bucket_name,
            Key: job_id.concat('_', task.step, '_', task.instance_id),
        }

        //Fetch data from AWS
        var params = {
            Bucket: task.bucket_name,
            Key: task.object_key,
            Range: 'bytes='.concat(task.start_index, '-', task.end_index)
        };

        self.s3.getObject(params, function (err, output_data) {
            if (err) {
                self.log("Error in fetching data");
                self.log(err, err.stack); // an error occurred
            }
            else {
                self.s3.getObject(state_params, function (err, data) {
                    if (err) {
                        self.log("Error in fetching state");
                        self.log(err, err.stack); // an error occurred
                        self.runAndUpload(output_data);
                    }
                    else {
                        // successful response
                        self.current_state = JSON.parse(data.Body.toString());
                        self.log("Fetched state is: ");
                        self.log(self.current_state);
                        self.runAndUpload(output_data);
                    }
                });
            }
        });
    },

    runAndUpload: function (data) {
        var output = "";

        var context = {
            write: function (key, value) {
                output = output.concat(key, ":", value, "\n");
            },
            state: {}
        }
        context.state = this.current_state;

        var data_chunk;
        data_chunk = data.Body.toString();
        var split_data = data_chunk.split("\n");

        var runner = runMap();
        if (typeof runner.setup == 'function')
            runner.setup(context);
        //run code on each line of data
        for (var i = 0; i < split_data.length; i++) {
            var key = i + 1;
            var value = split_data[i];
            runner.run(key, value, context);
        }
        this.current_output = output;
        this.current_state = context.state;
        //upload data to s3
        this.uploadData();
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
            this.log("Can't run Task `" + task.task_id + "`, client not registered", true);
        }

        // TODO In case of multiple tasks stop and cleanup the current task before starting a new one
        //cleanup this.current_task
        // Update current task with new one.
        this.current_task = task;

        /*
         if(this.step_list.indexof(this.current_task.step) == -1)
         {
         this.step_list.push(this.current_task.step);
         this.getCode();
         }
         */
        //var p = this.getCode();
        
        // This function does a bunch of stuff:
        //  1. get's the code for this step
        //  2. get's the data for this task
        //  3. runs the task over the data
        //  4. uploads the results to AWS and send the result to the server
        this.getCode();
    },

    getCode: function () {
        var params = {
            client_id: this.client_id,
            auth_token: this.auth_token,
            job_id: this.current_task.job_id,
            step: this.current_task.step,
            return_func: 'runMap',
        }
        this.getScript(
            this.url('code?' + decodeURIComponent(jQuery.param(params))),
            this.getData // get the data and run task after code is ready
        );
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
                job_id: this.current_task ? this.current_task.job_id : null
            },
            function (data) {
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
    loadAndRegister: function () {
        var all_loaded = true;
        if (typeof AWS == 'undefined') {
            this.getScript(this.aws_sdk, function (self) {
                self.requirements.AWS = true;
                self.register(self);
            });
            all_loaded = false;
        } else {
            this.requirements.AWS = true;
        }

        if (typeof jQuery == 'undefined') {
            this.getScript(this.options.jquery_cdn, function (self) {
                self.requirements.jQuery = true;
                self.register(self);
            });
            all_loaded = false;
        } else {
            this.requirements.jQuery = true;
        }

        if (all_loaded)
            this.register(this);
    },

    /**
     * Create a API url by prepending the server path.
     * @param  {String} path API call path
     * @return {String}      Full API path.
     */
    url: function (path) {
        return this.options.server_path + '/' + path;
    },

    /**
     * Log a message to console if debug mode is on
     * @param  {String}  message Message to log
     * @param  {Boolean} error   Is this an error or not.
     */
    log: function (message, error) {
        if (this.options.debug) {
            if (error == true)
                console.error("JsMr: " + message);
            else
                console.log("JsMr: " + message);
        }
    },

    getScript: function (url, success) {
        var _this = this;
        var script = document.createElement('script');
        script.src = url;
        var head = document.getElementsByTagName('head')[0],
            done = false;
        // Attach handlers for all browsers
        script.onload = script.onreadystatechange = function () {
            if (!done && (!this.readyState || this.readyState == 'loaded' || this.readyState == 'complete')) {

                done = true;
                // callback function provided as param
                if (typeof success != 'undefined')
                    success(_this);
                script.onload = script.onreadystatechange = null;
                head.removeChild(script);
            }
            ;
        };
        head.appendChild(script);
    },

    /**
     * Updates the last activity of this client. used to avoid making extra
     * API calls.
     */
    serverCalled: function () {
        this.last_call = jQuery.now();
    }
});
