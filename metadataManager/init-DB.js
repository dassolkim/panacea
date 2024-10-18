// const sequelize = require('sequelize');
const sequelize  = require('../models/index').sequelize;
const config = require('../config/config.json')
const env = config.development

// let sequelize = new Sequelize(env.database, env.username, env.password, env);

sequelize.sync({force: false}).then(()=>{
    console.log("DB connecting succeeded")
}).catch((err)=> {
    console.log(err)
})