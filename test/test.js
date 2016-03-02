var test = require('tape'),
    righto = require('righto'),
    noop = function(){};

module.exports = function(name, options, task){
    if(arguments.length < 3){
        task = options;
        options = null;
    }

    var last = righto.sync(noop);

    test(name, options, function(t){
        last = righto(function(done){
            t.on('end', done);

            task(t, done);

        }, [last]);

        last(noop);
    });
};

module.exports.only = test.only;