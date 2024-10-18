const Sequelize = require('sequelize')


module.exports = class DatasetFormat extends Sequelize.Model {
    static init(sequelize) {
        return super.init({
            id: {
                type: Sequelize.UUID,
                allowNull: false,
                primaryKey: true
            },
            dataset_id: {
                type: Sequelize.UUID,
                allowNull: false
            },
            format_id: {
                type: Sequelize.UUID,
                allowNull: false
            },
            theme_id: {
                type: Sequelize.UUID,
                allowNull: true
            },
            state: {
                type: Sequelize.STRING(10)
            }
        }, {
            sequelize,
            timestamps: true,
            modelName: 'DatasetFormat',
            tableName: 'dataset_format',
            charset: 'utf8'
        })
    }
    static associate(db) {
        db.DatasetFormat.hasMany(db.Dataset, {foreignKey: 'dataset_id', sourceKey: 'id'});
        db.DatasetFormat.hasMany(db.Format, {foreignKey: 'format_id', sourceKey: 'id'});
        db.DatasetFormat.hasMany(db.Theme, {foreignKey: 'theme_id', sourceKey: 'id'});
    }
}
