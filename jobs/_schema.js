var m1 = function () {
    /**
     * This flag indicates whether this is a mapper or a reducer. In the case of
     * a reducer, this.run() is called with a list of values instead of a single
     * value.
     * @type {Boolean}
     */
    this.is_reduce = false;

    /**
     * Number of instances to create for this mapper.
     * -1 would mean as many as split chunks.
     * @type {Number}
     */
    this.instances = 1;

    /**
     * Called on initialization of each instnace of this step
     * @param  {Object} context context of this step
     * @return {void}
     */
    this.startup = function (context) {
        // stub
    };

    /**
     * Called per record of the input
     * @param  {string}              key     key of the input record
     * @param  {string|list[string]} value   value (or list of values) of the input record,
     * @param  {Object}              context context of this step of the chain
     * @return {void}
     */
    this.run = function (key, value, context) {
        // stub
    };

    /**
     * called once per instance of this step after all input is seen.
     * @param  {Object} context context of this step
     * @return {void}
     */
    this.cleanupInstnace = function (context) {
        // stub
    };

    /**
     * called once for this step, gets the state of all instances.
     * @param  {list[Object]} states  list of states of all instances
     * @param  {Object}       context context of this step
     * @return {void}
     */
    this.cleanupState = function (states, context) {
        // stub
    };
}

var m2 = function () {
    // Another step definition as outlined above
};

/**
 * Must be set to the job description
 * @type {Object}
 */
module.exports = {
    chain: [m1, m2],
    input: {
        type: 'local', // or AWS

        // for local:
        path: '/path/to/local/file',
        // for AWS:
        bucket: 'bucket_name',
        key: 'file_key',
    },
    output: {
        type: 'local', // or AWS

        // for local:
        path: '/path/to/local/dir',

        // for AWS:
        bucket: 'bucket_name',
        key: 'dir_key',
    },
    options: { // this will be available in the context
        _sys_option: 'sys_value', // _sys_ options are reserved for the framework.
        options_1: 'value_one',
        options_2: false,
        // ...
    }
}