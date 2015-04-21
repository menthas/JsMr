var m1 = function () {
    this.is_reduce = false;

    this.instances = 5;

    this.run = function (key, value, context) {
        var line_split = value.split(' ');
        var count = 0;
        for(var i in line_split){
            context.write(line_split[i], 1);
        }
    };

    return this;
}

var r1 = function () {
    this.is_reduce = true;
    this.instances = -1;

    this.setup = function (context) {
        if (!context.state.TotalCount)
            context.state.TotalCount = 0;
    };

    this.run = function (key, values, context) {
        context.state.key = key;
        for (var i=0; i<values.length; i++)
            context.state.TotalCount += parseInt(values[i]);
    }

    this.cleanup = function (states, context) {
        for (var i=0; i<states.length; i++)
            context.write(states[i].key, states[i].TotalCount);
    }

    return this;
}

/**
 * Must be set to the job description
 * @type {Object}
 */
module.exports = {
    chain: [m1, r1],
    input: {
        type: 'AWS', // or AWS
        bucket: 'bucket_name',
        key: 'file_key',
    },
    output: {
        type: 'AWS', // or AWS
        bucket: 'bucket_name',
    },
    options: {}
}
