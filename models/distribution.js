const Sequelize = require('sequelize')

module.exports = class Distribution extends Sequelize.Model {
    static init(sequelize) {
        return super.init({
            id: {
                type: Sequelize.UUID,
                allowNull: false,
                primaryKey: true
            },
            url: {
                type: Sequelize.STRING(),
                allowNull: false
            },
            title: {
                type: Sequelize.STRING(),
                allowNull: true,
                unique: false
            },
            name: {
                type: Sequelize.STRING(),
                allowNull: true,
                unique: false
            },
            identifier: {
                type: Sequelize.STRING(),
                allowNull: true
                // unique: true
            },
            publisher: {
                type: Sequelize.STRING(),
                allowNull: false,
                unique: false
            },
            dataset_id: {
                type: Sequelize.UUID,
                allowNull: false
            },
            datalake_id: {
                type: Sequelize.UUID
            },
            mediatype: {
                type: Sequelize.STRING(),
                allowNull: true
            },
            format: {
                type: Sequelize.STRING(),
                allowNull: false
            },
            migration_status: {
                type: Sequelize.STRING(20),
                allowNull: true
            }
        }, {
            sequelize,
            timestamps: true,
            modelName: 'Distribution',
            tableName: 'distribution',
            charset: 'utf8'
        })
    }
    static associate(db) {
        db.Distribution.hasMany(db.Dataset, {foreignKey: 'dataset_id', sourceKey: 'id'});
        db.Distribution.hasMany(db.Datalake, {foreignKey: 'datalake_id', sourceKey: 'id'});
    }
}
