'use strict';

const fs = require('fs');
const path = require('path');
const Sequelize = require('sequelize');
const process = require('process');
const Datalake = require('./datalake')
const Dataset = require('./dataset')
const DatasetKeyword = require('./dataset_keyword')
const Distribution = require('./distribution')
const Keyword = require('./keyword')
const Format = require('./format')
const DatasetFormat = require('./dataset_format')
const Theme = require('./theme')

const basename = path.basename(__filename);
const env = process.env.NODE_ENV || 'development';
const config = require(__dirname + '/../config/config.json')[env];
const db = {};

let sequelize;
if (config.use_env_variable) {
  sequelize = new Sequelize(process.env[config.use_env_variable], config);
} else {
  sequelize = new Sequelize(config.database, config.username, config.password, config);
}

// fs
//   .readdirSync(__dirname)
//   .filter(file => {
//     return (file.indexOf('.') !== 0) && (file !== basename) && (file.slice(-3) === '.js');
//   })
//   .forEach(file => {
//     const model = require(path.join(__dirname, file))(sequelize, Sequelize.DataTypes);
//     db[model.name] = model;
//   });

// Object.keys(db).forEach(modelName => {

//   if (db[modelName].associate) {
//     db[modelName].associate(db);
//   }
// });

db.sequelize = sequelize;
db.Sequelize = Sequelize;

db.Datalake = Datalake;
db.Dataset = Dataset;
db.DatasetKeyword = DatasetKeyword;
db.Distribution = Distribution;
db.Keyword = Keyword;
// db.User = User;
db.DatasetFormat = DatasetFormat;
db.Format = Format;
db.Theme = Theme;

/* create tables */

Datalake.init(sequelize)
Dataset.init(sequelize)
DatasetKeyword.init(sequelize)
Distribution.init(sequelize)
Keyword.init(sequelize)
// User.init(sequelize)
DatasetFormat.init(sequelize)
Format.init(sequelize)
Theme.init(sequelize)

/* update foreign key constraints */

// DatasetKeyword.associate(sequelize)
// Distribution.associate(sequelize)
// Keyword.associate(db)
// Dataset.associate(db)
// DatasetFormat.associate(sequelize)

module.exports = db;
// module.exports = Sequelize;