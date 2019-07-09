var AWS = require('aws-sdk'),
    righto = require('righto'),
    merge = require('merge'),
    shuv = require('shuv'),
    deepEqual = require('deep-equal'),
    errors = require('generic-errors');

var PAY_PER_REQUEST = 'PAY_PER_REQUEST';
var PROVISIONED = 'PROVISIONED';

function resolve(value){
    return righto(function(value, done){

        if(value && (typeof value === 'object' || typeof value === 'function')){
            var results = {};

            righto.all(Object.keys(value).map(function(key){
                return righto.sync(function(result){
                    results[key] = result;
                }, resolve(value[key]))
            }))(function(error){
                if(error){
                    return done(error);
                }

                done(null, results);
            });

            return;
        }

        done(null, value);
    }, value);
}

function createKey(table, value){
    var key = {};

    key[table.keyField] = value;

    return key;
}

function getOptions(table, options){
    return righto(function(table, options, done){
        if(typeof options !== 'object'){
            if(!table.keyField){
                return done(new Error('Could not construct complex key from ' + options));
            }
            return done(null, {
                key: createKey(table, options)
            });
        }

        if(options.key && typeof options.key !== 'object'){
            if(!table.keyField){
                return done(new Error('Could not construct complex key from ' + options));
            }
            options.key = createKey(table, options.key);
        }

        done(null, options);
    }, table, resolve(options));
}

function get(tableContext, options, callback) {
    var table = tableContext.table;

    function dbGet(table, options, done){
        tableContext.context.docClient.get({
            Key: options.key,
            ConsistentRead: options.consistentRead,
            TableName: table.name
        }, done);
    }

    var entity = righto(dbGet, table, getOptions(table, options)).get('Item');

    var result = righto(function(record, done){
        if(!record){
            return done(new errors.NotFound());
        }
        done(null, record);
    }, entity);

    callback && result(callback);

    return result;
}

function create(tableContext, options, callback){
    var table = tableContext.table;

    var entity = righto(function(table, options, done){
        var item = options.item;

        tableContext.context.docClient.put({
            Item: item,
            AttributeUpdates: options.attributeUpdates,
            ConditionExpression: options.conditionExpression,
            ExpressionAttributeValues: options.conditionValues,
            ExpressionAttributeNames: options.conditionNames,
            TableName: table.name
        }, function(error){
            return done(error, !error && item);
        });
    }, table, getOptions(table, options));

    callback && entity(callback);

    return entity;
}

function createAttributeValues(data){
    var result = {};

    for(var key in data){
        result[':' + key] = data[key];
    }

    return result;
}

function createAttributeNames(data){
    var result = {};

    for(var key in data){
        result['#' + key] = key;
    }

    return result;
}

function createUpdateExpression(data){
    var result = [];

    for(var key in data){
        result.push('#' + key + ' = :' + key);
    }

    return 'SET ' + result.join(', ');
}

function update(tableContext, options, callback){
    var table = tableContext.table;

    function dbUpdate(table, options, done){
        var item = options.item,
            attributeValues = options.attributeValues,
            attributeNames = options.attributeNames,
            expression = options.expression;

        if(!expression){
            expression = createUpdateExpression(item);
            attributeValues = createAttributeValues(item);
            attributeNames = createAttributeNames(item);
        }

        tableContext.context.docClient.update({
            Key: options.key,
            UpdateExpression: expression,
            ExpressionAttributeValues: attributeValues,
            ExpressionAttributeNames: attributeNames,
            ReturnValues: 'ALL_NEW',
            TableName: table.name
        }, done);
    }

    var update = righto(dbUpdate, table, getOptions(table, options));

    callback && update(callback);

    return update;
}

function formatMultiResult(result){
    return {
        count: result.Count,
        rows: result.Items,
        scannedCount: result.ScannedCount
    };
}

function findAll(tableContext, options, callback) {
    var table = tableContext.table;

    function dbQuery(table, options, done){
        tableContext.context.docClient.query({
            KeyConditionExpression: options.expression,
            Limit: options.limit,
            ScanIndexForward: options.forward,
            ExpressionAttributeNames: options.attributeNames,
            ExpressionAttributeValues: options.attributeValues,
            TableName: table.name
        }, done);
    }

    var result = righto(dbQuery, table, getOptions(table, options)).get(formatMultiResult);

    callback && result(callback);

    return result;
}

function scan(tableContext, options, callback) {
    var table = tableContext.table;

    function dbScan(table, options, done){

        var expression = options.expression,
            attributeValues = options.attributeValues;

        tableContext.context.docClient.scan({
            FilterExpression: expression,
            Limit: options.limit,
            ScanIndexForward: options.forward,
            ExpressionAttributeNames: options.attributeNames,
            ExpressionAttributeValues: attributeValues,
            ConsistentRead: options.consistentRead,
            TableName: table.name
        }, done);
    }

    var result = righto(dbScan, table, getOptions(table, options)).get(formatMultiResult);

    callback && result(callback);

    return result;
}

function remove(tableContext, options, callback){
    var table = tableContext.table;

    var remove = righto(function(table, options, done){
        tableContext.context.docClient.delete({
            Key: options.key,
            TableName: table.name
        }, done);
    }, table, getOptions(table, options));

    var result = righto(function(done){
        done(null);
    }, righto.after(remove));

    callback && result(callback);

    return result;
}

var tableMethods = {
    get: get,
    findAll: findAll,
    scan: scan,
    create: create,
    update: update,
    remove: remove
};

var awsTypes = {
    'hash':'HASH',
    'range':'RANGE',
    'string':'S',
    'number':'N',
    'binary':'B'
}

var localTypes = Object.keys(awsTypes).reduce(function(result, key){
    result[awsTypes[key]] = key;
    return result;
}, {});

function createLocalTable(context, config){
    var table = {
            alternator: context.alternator,
            name: config.name,
            key: config.key,
            attributes: config.attributes,
            range: config.range
        };

    var keyFields = Object.keys(config.key);

    if(keyFields.length === 1){
        table.keyField = keyFields[0];
    }

    return table;
}

function removeLocalTable(context, tableName){
    delete context.tables[tableName];
}

function createTable(context, definition, callback){

    var keySchema = Object.keys(definition.key).map(function(key){
            return {AttributeName: key, KeyType: awsTypes[definition.key[key]]}
        }),
        attributeDefinitions = Object.keys(definition.attributes).map(function(key){
            return {AttributeName: key, AttributeType: awsTypes[definition.attributes[key]]}
        });

    function createDbTable(config, done){
        var billingMode = definition.billingMode || PAY_PER_REQUEST;
        var provisionedThroughput = definition.provisionedThroughput || {
                ReadCapacityUnits: 1,
                WriteCapacityUnits: 1
            };

        var tableSettings = {
            TableName : config.name,
            KeySchema: keySchema,
            AttributeDefinitions: attributeDefinitions,
            BillingMode: billingMode
        };

        if(billingMode === PROVISIONED){
            tableSettings.ProvisionedThroughput = provisionedThroughput
        }

        var createdTable = context.dynamodb.createTable(tableSettings, done);
    }

    var createTable = righto(createDbTable, definition);

    context.tables[definition.name] = righto.sync(createLocalTable, context, definition, [createTable]);

    callback && createTable(callback);

    return createTable;
}

function deleteTable(context, name, callback){
    var deleteTable = righto(function(done){
            context.dynamodb.deleteTable({
                TableName: name
            }, done);
        });

    var removeTable = righto.sync(function(){
        removeLocalTable(context, name);
    }, righto.after(deleteTable));

    callback && removeTable(callback);

    return removeTable;
}

function listTables(context, callback){
    function dbListTables(done){
        context.dynamodb.listTables(function(error, names){
            if(error){
                return done(error);
            }

            // Always returns with 'table_name' in the list for some reason? ¯\_(ツ)_/¯
            done(null, names.TableNames.filter(function(name){
                return name !== 'table_name'
            }));
        });
    }

    var tableNames = righto(dbListTables);

    callback && tableNames(callback);

    return tableNames;
}

function describeTable(context, tableName, callback){
    function dbDescribeTable(tableName, done){
        context.dynamodb.describeTable({
            TableName: tableName
        }, done);
    }

    var table = righto(dbDescribeTable, tableName);

    callback && table(callback);

    return table;
}

function tableKeyToLocal(keySchema){
    return keySchema.reduce(function(result, schema){
        result[schema.AttributeName] = localTypes[schema.KeyType];

        return result;
    }, {});
}

function tableAttributesToLocal(attributeDefinitions){
    return attributeDefinitions.reduce(function(result, definition){
        result[definition.AttributeName] = localTypes[definition.AttributeType];

        return result;
    }, {});
}

function compareTable(table, config, callback){
    var compare = righto(function(table, config, done){

        var error = Object.keys(config).reduce(function(error, key){

            if(key === 'key'){
                var remoteKey = tableKeyToLocal(table.KeySchema);
                if(!deepEqual(config.key, remoteKey)){
                    return new Error('key did not match - local: ' + JSON.stringify(config.key) + ' remote: ' + JSON.stringify(remoteKey));
                }
            }

            if(key === 'attributes'){
                var remoteAttributes = tableAttributesToLocal(table.AttributeDefinitions);
                if(!deepEqual(config.attributes, remoteAttributes)){
                    return new Error('key did not match - local: ' + JSON.stringify(config.attributes) + ' remote: ' + JSON.stringify(remoteAttributes));
                }
            }

            return error;
        }, null);

        done(error);
    }, table, config);

    callback && compare(callback);

    return compare;
}

function syncTables(context, dbTables, tableConfigs){
    return righto(function(dbTables, done){
        var dbTableMap = dbTables.reduce(function(results, dbTable){
            results[dbTable.Table.TableName] = dbTable.Table;

            return results;
        }, {});


        var synced = righto.all(tableConfigs.map(function(config){

            var table;

            if(config.name in dbTableMap){
                table = compareTable(dbTableMap[config.name], config);
            }else{
                table = createTable(context, config);
            }

            context.tables[config.name] = righto.sync(createLocalTable, context, config, righto.after(table));
            return table;

        }));

        synced(function(error){
            done(error);
        });
    }, dbTables);
}

function getTable(context, tableName){
    var table = {
        alternator: context.alternator,
        name: tableName
    };

    return Object.keys(tableMethods).reduce(function(table, key){
        var localTable =
            context.tables[tableName] ||
            righto(function(done){
                if(!context.tables[tableName]){
                    return done(new Error('No table named ' + tableName + ' has been described'));
                }

                context.tables[tableName](done);
            }, righto.after(context.tablesSynced));

        table[key] = shuv(tableMethods[key], {
            context: context,
            table: localTable
        });

        return table;
    }, table);
}

function createDb(awsConfig, tableConfigs){
    var alternator = {},
        context = {
            dynamodb: new AWS.DynamoDB(awsConfig),
            docClient: new AWS.DynamoDB.DocumentClient(awsConfig),
            alternator: alternator,
            tables: {},
        };

    var dbTableNames = listTables(context);
    var dbTables = righto.all(righto.sync(function(tableInfo, done){
            return tableInfo.map(shuv(describeTable, context, shuv._, shuv.$));
        }, dbTableNames));

    context.tablesSynced = syncTables(context, dbTables, tableConfigs)

    alternator.table = shuv(getTable, context);
    alternator.createTable = shuv(createTable, context);
    alternator.deleteTable = shuv(deleteTable, context);
    alternator.listTables = shuv(listTables, context);


    alternator.ready = context.tablesSynced;

    // Immediatly sync
    context.tablesSynced(function(error){
        if(error){
            throw error;
        }
    });


    return alternator;
}

module.exports = createDb;