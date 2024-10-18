const Sequelize = require('sequelize');

module.exports = class Datalake extends Sequelize.Model {
    static init(sequelize) {
        return super.init({
            id: {
                type: Sequelize.UUID,
                allowNull: false,
                primaryKey: true
            },
            bucket: {
                type: Sequelize.STRING(256),
                allowNull: false
            },
            type: {
                type: Sequelize.STRING(20),
                allowNull: false,
            },
            state: {
                type: Sequelize.STRING(20)
            },
            size: {
                type: Sequelize.BIGINT()
            },
        }, {
            sequelize,
            timestamps: true,
            modelName: 'Datalake',
            tableName: 'datalake',
            charset: 'utf8'
        })
    }
}
