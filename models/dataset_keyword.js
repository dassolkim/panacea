const Sequelize = require('sequelize')


module.exports = class DatasetKeyword extends Sequelize.Model {
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
            keyword_id: {
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
            modelName: 'DatasetKeyword',
            tableName: 'dataset_keyword',
            charset: 'utf8'
        })
    }
    static associate(db) {
        db.DatasetKeyword.hasMany(db.Dataset, {foreignKey: 'dataset_id', sourceKey: 'id'});
        db.DatasetKeyword.hasMany(db.Keyword, {foreignKey: 'keyword_id', sourceKey: 'id'});
        db.DatasetKeyword.hasMany(db.Theme, {foreignKey: 'theme_id', sourceKey: 'id'});
    }
}
