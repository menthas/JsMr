Sequelize = require('sequelize');

var server = module.parent.exports.server;
var storage = module.parent.exports.storage;

/**
 * Returns information used for the admin dashboard page.
 */
server.get('/admin/dashboard', function (req, res, next) {
    var chain = new Sequelize.Utils.QueryChainer();
    chain.add(storage.Client.count())
         .add(storage.Job.count())
         .run()
         .success(function (result) {
            res.json({
                'clients': result[0],
                'jobs': result[1],
            });
            next();
         })
         .error(function () {
            res.json({error:true});
            next();
         });
});
