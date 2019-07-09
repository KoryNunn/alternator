var test = require('./test'),
    alternator = require('../'),
    righto = require('righto');

var AWS = require('aws-sdk');

var awsConnectionConfig = {
        endpoint: new AWS.Endpoint('http://localhost:8000'),
        accessKeyId: 'myKeyId',
        secretAccessKey: 'secretKey',
        region: 'us-east-1'
    };

function deleteTable(db, tableName){
    var waitABit = righto(function(done){
            setTimeout(done, 50);
        });

    var tableList = righto(db.listTables, righto.after(waitABit));

    return righto(function(tables, done){
        if(~tables.indexOf(tableName)){
            return db.deleteTable(tableName, done);
        }

        done();
    }, tableList);
}

function createDb(customTable, callback){
    if(arguments.length < 2){
        callback = customTable;
        customTable = null;
    }

    var db = alternator(awsConnectionConfig, []);

    var deleteTest = deleteTable(db, 'test');

    var createTest = righto(db.createTable, customTable || {
        name: 'test',
        key: {
            id:'hash'
        },
        attributes: {
            id:'string'
        }
    }, righto.after(deleteTest));

    createTest(function(error){
        if(error){
            throw error;
        }
        callback(error, db);
    });
}

function createId(){
    return righto.sync(function(){
        return String(Math.floor(Math.random() * 1e12));
    });
}

test('init alternator', function(t){
    t.plan(2);

    var initialDb = alternator(awsConnectionConfig, []);

    var deleteTest = deleteTable(initialDb, 'test');

    function createFirstConnection(){
        return alternator(awsConnectionConfig, [
            {
                name: 'test',
                key: {
                    id: 'hash'
                },
                attributes: {
                    id: 'string'
                }
            }
        ]);
    }

    var first = righto(function(done){
        createFirstConnection().ready(done);
    }, righto.after(deleteTest));

    var second = righto(function(done){
        createFirstConnection().ready(done);
    }, righto.after(first));

    var third = righto(function(done){
        alternator(awsConnectionConfig, [
            {
                name: 'test',
                key: {
                    id: 'hash',
                    foo: 'range'
                },
                attributes: {
                    id: 'string',
                    foo: 'number'
                }
            }
        ]).ready(done);
    }, righto.after(second));

    initialDb.ready(function(){

        first(function(error){
            t.notOk(error, 'initial setup should work');
        })

        second(function(error){
            t.notOk(error, 'identical setup should work');
        });

        /*
        This doesnt work for some reason

        var d = require('domain').create();

        d.on('error', function(error){
            t.ok(error, 'different setup should error');
        });

        d.run(function(){
            third(function(){})
        });
        */

    });

});

test('create/get item', function(t){
    t.plan(3);

    createDb(function(error, db){

        var newItem = db.table('test').create({
                item: {
                    id: createId(),
                    foo: 'bar'
                }
            });

        var retrievedItem = db.table('test').get(newItem.get('id'));

        newItem(function(error, item){
            t.notOk(error);
        });

        retrievedItem(function(error, item){
            t.notOk(error);

            t.equal(item.foo, 'bar');
        });

    });

});

test('create item with expression', function(t){
    t.plan(3);

    createDb(function(error, db){

        var newItem = db.table('test').create({
            item:{
                    id: createId(),
                    foo: 'bar'
                }
            });

        var retrievedItem = db.table('test').get(newItem.get('id'));

        newItem(function(error, item){
            t.notOk(error);
        });

        retrievedItem(function(error, item){
            t.notOk(error);

            t.equal(item.foo, 'bar');
        });

    });

});

test('remove item', function(t){
    t.plan(6);

    createDb(function(error, db){

        var newItem = db.table('test').create({
            item:{
                id: createId(),
                foo: 'bar'
            }
        });

        var retrievedItem = db.table('test').get(newItem.get('id'));

        var removeItem = db.table('test').remove(retrievedItem.get('id'));

        var retrievedItemAfterDelete = righto(db.table('test').get, retrievedItem.get('id'), righto.after(removeItem));

        newItem(function(error, item){
            t.notOk(error);
        });

        retrievedItem(function(error, item){
            t.notOk(error);

            t.equal(item.foo, 'bar');
        });

        removeItem(function(error, item){
            t.notOk(error);
        });

        retrievedItemAfterDelete(function(error, item){
            t.ok(error);
            t.notOk(item);
        });

    });


});

test('update item', function(t){
    t.plan(7);

    createDb(function(error, db){

        var newItem = db.table('test').create({
                item:{
                    id: createId(),
                    foo: 'bar'
                }
            }),
            testItem = db.table('test').get(newItem.get('id')),
            updateItem = db.table('test').update({
                key: testItem.get('id'),
                item: {foo: 'baz'}
            }),
            updatedItem = righto(db.table('test').get, testItem.get('id'), righto.after(updateItem));

        newItem(function(error, item){
            t.notOk(error, 'created item');
            t.deepEqual(item, {id: item.id, foo: 'bar'});
        });

        testItem(function(error, item){
            t.notOk(error, 'retrieved item');
            t.deepEqual(item, {id: item.id, foo: 'bar'});
        });

        updateItem(function(error){
            t.notOk(error, 'updated item');
        });

        updatedItem(function(error, item){
            t.notOk(error, 'retrieve after update');
            t.deepEqual(item, {id: item.id, foo: 'baz'});
        });

    });
});

test('update item with expression', function(t){
    t.plan(7);

    createDb(function(error, db){

        var newItem = db.table('test').create({
                item:{
                    id: createId(),
                    version: 0
                }
            }),
            testItem = db.table('test').get(newItem.get('id')),
            updateItem = db.table('test').update({
                key: testItem.get('id'),
                expression: 'ADD version :x',
                attributeValues: {
                    ':x': 1
                }
            }),
            updatedItem = righto(db.table('test').get, testItem.get('id'), righto.after(updateItem));

        newItem(function(error, item){
            t.notOk(error, 'created item');
            t.deepEqual(item, {id: item.id, version: 0});
        });

        testItem(function(error, item){
            t.notOk(error, 'retrieved item');
            t.deepEqual(item, {id: item.id, version: 0});
        });

        updateItem(function(error){
            t.notOk(error, 'updated item');
        });

        updatedItem(function(error, item){
            t.notOk(error, 'retrieve after update');
            t.deepEqual(item, {id: item.id, version: 1});
        });

    });
});

test('findAll', function(t){
    t.plan(3);

    createDb({
            name: 'test',
            key: {
                id: 'hash',
                age: 'range'
            },
            attributes: {
                id: 'string',
                age: 'number'
            }
        }, function(error, db){

        var id = createId();

        var newItem1 = db.table('test').create({
                item:{
                    id: id,
                    foo:'bar',
                    age: 10
                }
            }),
            newItem2 = db.table('test').create({
                item:{
                    id: id,
                    foo:'baz',
                    age: 20
                }
            }),
            newItem3 = db.table('test').create({
                item:{
                    id: createId(),
                    foo:'baz',
                    age: 20
                }
            }),
            allItems = righto.all(newItem1, newItem2, newItem3),
            findItems = righto(db.table('test').findAll, {
                expression: 'id = :id AND age > :min',
                consistentRead: true,
                attributeValues: {
                    ':id': id,
                    ':min': 15
                }
            }, righto.after(allItems));

        findItems(function(error, result){
            t.notOk(error, 'no error');

            t.deepEqual(result.count, 1);
            t.deepEqual(result.rows.length, 1);
        });

    });
});


test('scan', function(t){
    t.plan(2);

    createDb(function(error, db){

        var newItem1 = db.table('test').create({
                item: {
                    id: createId(),
                    foo:'bar'
                }
            }),
            newItem2 = db.table('test').create({
                item:{
                    id: createId(),
                    foo:'baz'
                }
            }),
            findItems = righto(db.table('test').scan, {
                expression: 'foo = :foo',
                consistentRead: true,
                attributeValues: {
                    ':foo': 'bar'
                }
            }, righto.after(newItem1, newItem2));

        var items = righto.sync(function(item1, foundItems){
            return {
                item1: item1,
                foundItems: foundItems
            };
        }, newItem1, findItems);

        items(function(error, result){
            t.notOk(error, 'no error');

            t.deepEqual(result.foundItems.rows, [result.item1]);
        });

    });
});



test('alternator hash + range', function(t){
    t.plan(4);

    var db = alternator(awsConnectionConfig, []);

    db.ready(function(error){

        var name = 'bob';

        var deleteUsers = deleteTable(db, 'users');

        var createUsers = righto(db.createTable, {
            name: 'users',
            key: {
                name: 'hash',
                version: 'range'
            },
            attributes: {
                name: 'string',
                version: 'number'
            }
        }, righto.after(deleteUsers));

        var newItem1 = righto(db.table('users').create, {
                item: {
                    name: name,
                    version: 1
                }
            }, righto.after(createUsers)),
            newItem3 = righto(db.table('users').create, {
                item: {
                    name: name,
                    version: 3
                }
            }, righto.after(createUsers)),
            newItem2 = righto(db.table('users').create, {
                item: {
                    name: name,
                    version: 2
                }
            }, righto.after(createUsers)),
            allItems = righto.all(newItem1, newItem2, newItem3),
            found = righto(db.table('users').findAll, {
                expression: '#name = :name',
                consistentRead: true,
                limit: 1,
                forward: false, // begin searching from the last index
                attributeNames: {
                    '#name': 'name'
                },
                attributeValues: {
                    ':name': name
                }
            }, righto.after(allItems));

        found(function(error, result){
            t.notOk(error, 'no error');

            t.equal(result.count, 1);
            t.equal(result.rows.length, 1);
            t.equal(result.rows[0].version, 3);
        });

    });

});