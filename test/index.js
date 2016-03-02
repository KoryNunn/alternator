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

    var tableList = righto(db.listTables, [waitABit]);

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
    }, [deleteTest]);

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
    }, [deleteTest]);

    var second = righto(function(done){
        createFirstConnection().ready(done);
    }, [first]);

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
    }, [second]);

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

test('get document', function(t){
    t.plan(3);

    createDb(function(error, db){

        var newDocument = db.table('test').create({
                id: createId(),
                foo: 'bar'
            });

        var retrievedDocument = db.table('test').get(newDocument.get('id'));

        newDocument(function(error, document){
            t.notOk(error);
        });

        retrievedDocument(function(error, document){
            t.notOk(error);

            t.equal(document.foo, 'bar');
        });

    });

});

test('remove document', function(t){
    t.plan(6);

    createDb(function(error, db){

        var newDocument = db.table('test').create({
            id: createId(),
            foo: 'bar'
        });

        var retrievedDocument = db.table('test').get(newDocument.get('id'));

        var removeDocument = db.table('test').remove(retrievedDocument.get('id'));

        var retrievedDocumentAfterDelete = righto(db.table('test').get, retrievedDocument.get('id'), [removeDocument]);

        newDocument(function(error, document){
            t.notOk(error);
        });

        retrievedDocument(function(error, document){
            t.notOk(error);

            t.equal(document.foo, 'bar');
        });

        removeDocument(function(error, document){
            t.notOk(error);
        });

        retrievedDocumentAfterDelete(function(error, document){
            t.ok(error);
            t.notOk(document);
        });

    });


});

test('update document', function(t){
    t.plan(7);

    createDb(function(error, db){

        var newDocument = db.table('test').create({
                id: createId(),
                foo: 'bar'
            }),
            testDocument = db.table('test').get(newDocument.get('id')),
            updateDocument = db.table('test').update({
                key: testDocument.get('id'),
                data: {foo: 'baz'}
            }),
            updatedDocument = righto(db.table('test').get, testDocument.get('id'), [updateDocument]);

        newDocument(function(error, document){
            t.notOk(error, 'created document');
            t.deepEqual(document, {id: document.id, foo: 'bar'});
        });

        testDocument(function(error, document){
            t.notOk(error, 'retrieved document');
            t.deepEqual(document, {id: document.id, foo: 'bar'});
        });

        updateDocument(function(error){
            t.notOk(error, 'updated document');
        });

        updatedDocument(function(error, document){
            t.notOk(error, 'retrieve after update');
            t.deepEqual(document, {id: document.id, foo: 'baz'});
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

        var newDocument1 = db.table('test').create({
                id: id,
                foo:'bar',
                age: 10
            }),
            newDocument2 = db.table('test').create({
                id: id,
                foo:'baz',
                age: 20
            }),
            newDocument3 = db.table('test').create({
                id: createId(),
                foo:'baz',
                age: 20
            }),
            allDocuments = righto.all(newDocument1, newDocument2, newDocument3),
            findDocuments = righto(db.table('test').findAll, {
                expression: 'id = :id AND age > :min',
                consistentRead: true,
                attributeValues: {
                    ':id': id,
                    ':min': 15
                }
            }, [allDocuments]);

        findDocuments(function(error, result){
            t.notOk(error, 'retrieve after update');

            t.deepEqual(result.count, 1);
            t.deepEqual(result.rows.length, 1);
        });

    });
});


test('scan', function(t){
    t.plan(2);

    createDb(function(error, db){

        var newDocument1 = db.table('test').create({
                id: createId(),
                foo:'bar'
            }),
            newDocument2 = db.table('test').create({
                id: createId(),
                foo:'baz'
            }),
            findDocuments = righto(db.table('test').scan, {
                expression: 'foo = :foo',
                consistentRead: true,
                attributeValues: {
                    ':foo': 'bar'
                }
            }, [newDocument1], [newDocument2]);

        var documents = righto.sync(function(document1, foundDocuments){
            return {
                document1: document1,
                foundDocuments: foundDocuments
            };
        }, newDocument1, findDocuments);

        documents(function(error, result){
            t.notOk(error, 'retrieve after update');

            t.deepEqual(result.foundDocuments.rows, [result.document1]);
        });

    });
});



test.only('alternator hash + range', function(t){
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
        }, [deleteUsers]);

        var newDocument1 = righto(db.table('users').create, {
                name: name,
                version: 1
            }, [createUsers]),
            newDocument3 = righto(db.table('users').create, {
                name: name,
                version: 3
            }, [createUsers]),
            newDocument2 = righto(db.table('users').create, {
                name: name,
                version: 2
            }, [createUsers]),
            allDocuments = righto.all(newDocument1, newDocument2, newDocument3),
            findDocuments = db.table('users').findAll({
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
            });

        var found = righto(findDocuments, [allDocuments]);

        found(function(error, result){
            t.notOk(error, 'retrieve after update');

            t.equal(result.count, 1);
            t.equal(result.rows.length, 1);
            t.equal(result.rows[0].version, 3);
        });

    });

});