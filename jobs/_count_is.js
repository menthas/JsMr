var m1 = function () {
    this.is_reduce = false;

    this.instances = 5;

    this.setup = function (context) {
        context.state.TotalCount = 0;
    };

    this.run = function (key, value, context) {
        var line_split = value.split(' ');
        var count = 0;
        for(var i in line_split){
            if(line_split[i] == 'is') {
                context.state.TotalCount++;
                count++;
            }
        }
        context.write('is', count);
    };

    this.cleanup = function (states, context) {
        var total = 0;
        for (var i=0; i<states.length; i++) {
            total += states[i].TotalCount;
        }
        context.write("total", total);
    };

    return this;
}

/**
 * Must be set to the job description
 * @type {Object}
 */
module.exports = {
    chain: [m1],
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
