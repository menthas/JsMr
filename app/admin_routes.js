Sequelize = require('sequelize');

var server = module.parent.exports.server;
var storage = module.parent.exports.storage;
var runtime = module.parent.exports.runtime;

/**
 * Returns information used for the admin dashboard page.
 */
server.get('/admin/dashboard', function (req, res, next) {
    var chain = new Sequelize.Utils.QueryChainer();
    chain.add(storage.Client.count())
         .add(storage.Job.count())
         .run()
         .success(function (result) {
            var avg_uptime = runtime.client_count > 0 ?
                runtime.total_client_uptime / runtime.client_count : 0;
            res.json({
                'clients': result[0],
                'jobs': result[1],
                'avg_uptime': avg_uptime,
                'total_clients': runtime.client_count,
                'uptime': Math.floor((new Date() - runtime.uptime) / 1000)
            });
            next();
         })
         .error(function () {
            res.json({error:true});
            next();
         });
});
