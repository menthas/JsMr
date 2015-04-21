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

var offset_bytes = 40;

var JsMr = Class.extend({
    init: function () {

        this.registered = false;
        this.client_id = null;
        this.last_call = 0;
        this.current_task = null;
        this.current_output = "";
        this.current_state = {};

        this.beat_interval_obj = null;
        this.aws_sdk = 'https://sdk.amazonaws.com/js/aws-sdk-2.1.21.min.js';
        this.s3 = null;
        this.options = {
            beat_interval: 30000, // 30sec
            auth_token: '',
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

        var output_params = null;
        if (this.current_output) {
            var output_params = {
                Bucket: task.bucket_name, /* required */
                Key: job_id.concat('/_temp/', task.task_id),
                Body: this.current_output
            };
        }

        // only upload state if object and non-empty
        var state_params = null;
        if (this.current_state && typeof this.current_state == 'object') {
            var not_empty = false;
            for (item in this.current_state) {
                not_empty = true;
                break;
            }
            if (not_empty) {
                var state_params = {
                    Bucket: task.bucket_name, /* required */
                    Key: job_id.concat('/_temp/states/', task.step, '_', task.instance_id),
                    Body: JSON.stringify(this.current_state)
                };
            }
        }

        if (output_params) {
            this.s3.upload(output_params, function (err, data) {
                if (err)
                    _this.log(err, err.stack); // an error occurred
                else if (state_params) {
                    _this.s3.upload(state_params, function (err, data) {
                        if (err)
                            _this.log(err, err.stack); // an error occurred
                        else {
                            _this.updateTast(_this.current_task, "task_success");
                        }
                    });
                } else
                    _this.updateTast(_this.current_task, "task_success");
            });
        } else if (state_params) {
            this.s3.upload(state_params, function (err, data) {
                if (err)
                    _this.log(err, err.stack); // an error occurred
                else {
                    _this.updateTast(_this.current_task, "task_success");
                }
            });
        }
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
                if (data.registered == true) {
                    self.registered = true;
                    self.client_id = data.client_id;
                    try {
                        self.runTask(data.task);
                    } catch (err) {
                        self.log("Error occured during running the task. " + err);
                        self.updateTast(data.task, 'task_failure');
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
            this.url('task'),
            {
                client_id: this.client_id,
                auth_token: this.options.auth_token,
                task_id: task.task_id,
                action: action
            },
            function (data) {
                this.current_state = {};
                this.current_task = null;
                this.current_output = "";
                if (data.task)
                    _this.log("Got a new task (on update)");
                try {
                    _this.runTask(data.task);
                } catch (err) {
                    _this.log("Error occured during running the task. " + err);
                    _this.updateTast(data.task, 'task_failure');
                }
            }
        ).fail(function () {
            _this.log("Failed to update the task at " + _this.url('task'), true);
        });
        this.serverCalled();
    },

    getData: function (self) {
        var task = self.current_task;
        var credentials = {accessKeyId: task.access_key, secretAccessKey: task.secret_key};

        AWS.config.update(credentials);
        AWS.config.region = task.aws_region;
        self.s3 = new AWS.S3();

        // Go over special cases, currently only cleanup tasks
        if (task.special === 'cleanup') {
            self.log("Received a cleanup task, getting states and running.");
            return self.runSpecialCleanup(task);
        }

        var params = {
            Bucket: task.bucket_name,
            Key: task.object_key,
            Range: 0
        };

        //do not fetch the previous data since it is first task
        if (task.start_index == 0) {
            self.fetchDataAndStateFromAws(params, "");
        }
        else {
            self.fetchMissingData(task.start_index - offset_bytes, task.start_index, params);
        }

    },

    fetchDataAndStateFromAws: function (params, missing_line) {
        var self = this;
        var task = self.current_task;

        //Fetch state from AWS
        var job_id = task.job_id.toString();
        var state_params = {
            Bucket: task.bucket_name,
            Key: job_id.concat('/_temp/states/', task.step, '_', task.instance_id),
        }

        params.Range = 'bytes='.concat(task.start_index, '-', task.end_index);
        self.s3.getObject(params, function (err, output_data) {
            if (err) {
                self.log("Error in fetching data");
                self.log(err, err.stack); // an error occurred
                // TODO tell server task failed
                return;
            }
            var final_data_ = missing_line.concat(output_data.Body.toString());
            self.s3.getObject(state_params, function (err, data) {
                if (err && err.statusCode != 404) {
                    self.log("Error in fetching state");
                    self.log(err, err.stack); // an error occurred
                    // TODO tell server task failed
                    return;
                } else if (err) { // this is the first job, use empty state
                    self.log("First job, using clean state");
                    self.current_state = {};
                } else { // successful response
                    self.current_state = JSON.parse(data.Body.toString());
                    self.log("Fetched state is: " + data.Body.toString());
                }

                self.runAndUpload(final_data_);
            });

        });

    },

    fetchMissingData: function (start_index, end_index, params) {
        var self = this;
        var found_data = false;
        params.Range = 'bytes='.concat(start_index, '-', end_index);
        var missing_line = "";
        //Fetch missing data from AWS
        self.s3.getObject(params, function (err, missing_data) {
            if (err) {
                self.log("Error in fetching missing data");
                self.log(err, err.stack); // an error occurred
                // TODO tell server task failed
                return;
            }
            //check if the discarded line from previous task is fetched
            //completely.
            var data = missing_data.Body.toString();
            for (var i = data.length - 1; i >= 0; i--) {
                if (data[i] == "\n") {
                    missing_line = data.substring(i + 1, data.length - 1);
                    found_data = true;
                    break;
                }
            }
            if (!found_data) {
                return self.fetchMissingData(start_index - offset_bytes, end_index, params);
            }
            self.fetchDataAndStateFromAws(params, missing_line);
        });

    },

    runSpecialCleanup: function(task) {
        var _this = this;
        this.s3.listObjects({
            Bucket: task.bucket_name,
            Prefix: task.job_id + '/_temp/states'
        }, function (err, data) {
            var total_instances = data.Contents.length;
            var states = [];
            data.Contents.forEach(function (content) {
                _this.s3.getObject({
                    Bucket: task.bucket_name,
                    Key: content.Key,
                }, function (state_err, state_data) {
                    if (state_err && state_err.statusCode != 404) {
                        return this.log("Failed to load state on " + content);
                        // TODO tell server task failed
                    } else if (!state_err) {
                        states.push(JSON.parse(state_data.Body.toString()));
                    }
                    total_instances--;
                    if (total_instances == 0) { // all states are here, run
                        _this.log("Cleanup: All states fetched, running");
                        _this.runAndUploadStates(states);
                    }
                });
            });
        });
    },

    runAndUploadStates: function(states) {
        var output = "";
        var context = {
            write: function (key, value) {
                output = output.concat(key, ":", value, "\n");
            }
        }
        var runner = runMap();
        runner.cleanup(states, context);
        this.current_output = output;
        this.current_state = null;
        //upload data to s3
        this.uploadData();
    },

    runAndUpload: function (data) {
        var output = "";

        var context = {
            write: function (key, value) {
                output = output.concat(key, ":", value, "\n");
            },
            state: this.current_state
        }
        var split_data = data.split("\n");
        if (!split_data[split_data.length-1])
            split_data.pop(); // remove the extra \n from the end

        var runner = runMap();
        if (typeof runner.setup == 'function')
            runner.setup(context);

        if (runner.is_reduce) {
            // in case of reduce feed all the data at once
            runner.run(this.current_task.task_key, split_data, context);
        } else {
            //run code on each line of data and discard the last line
            for (var i = 0; i < split_data.length - 1; i++) {
                var key = i + 1;
                var value = split_data[i];
                runner.run(key, value, context);
            }
        }

        if (typeof runner.breakdown == 'function')
            runner.breakdown(context);

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
        // cleanup this.current_task
        // Update current task with new one.
        var need_code = !this.current_task ||
                        this.current_task.job_id != task.job_id ||
                        this.current_task.step != task.step;
        this.current_task = task;

        // This function does a bunch of stuff:
        //  1. get's the code for this step (if not the same as prev)
        //  2. get's the data for this task
        //  3. runs the task over the data
        //  4. uploads the results to AWS and send the result to the server
        this.getCode(need_code);
    },

    /**
     * Get the code required for this task if we haven't loaded it before.
     * @param  {boolean} need_code flag to indicate download
     */
    getCode: function (need_code) {
        if (need_code) {
            var params = {
                client_id: this.client_id,
                auth_token: this.options.auth_token,
                job_id: this.current_task.job_id,
                step: this.current_task.step,
                return_func: 'runMap',
            }
            this.getScript(
                this.url('code?' + decodeURIComponent(jQuery.param(params))),
                this.getData // get the data and run task after code is ready
            );
            this.serverCalled();
        } else {
            this.getData(this);
        }
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
                _this.log("Heartbeat ❤")
                if (data.task)
                    _this.log("Got a new task (on heartbeat)");
                try {
                    _this.runTask(data.task);
                } catch (err) {
                    _this.log("Error occured during running the task. " + err);
                    _this.updateTast(data.task, 'task_failure');
                }
            }, 'json'
        ).fail(function (jqXHR) {
            if (jqXHR.status == 404) {
                _this.log("Client was dropped by server. registering again.", true);
                _this.registered = false;
                _this.client_id = null;
                this.last_call = 0;
                clearInterval(_this.beat_interval_obj);
                _this.register(_this);
            } else {
                _this.log("Failed to sent heartbeat to " + _this.url('beat'), true);
            }
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
