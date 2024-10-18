const Sequelize = require('sequelize')


module.exports = class Dataset extends Sequelize.Model {
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
                // unique: true
            },
            title: {
                type: Sequelize.STRING(),
                allowNull: true,
                unique: false
            },
            description: {
                type: Sequelize.STRING(),
                allowNull: true,
                unique: false
            },
            issued: {
                type: Sequelize.DATE(),
                allowNull: true
            },
            modified: {
                type: Sequelize.DATE(),
                allowNull: true
            },
            identifier: {
                type: Sequelize.STRING(),
                allowNull: true
            },
            publisher: {
                type: Sequelize.STRING(100),
                allowNull: false,
                unique: false
            },
            theme: {
                type: Sequelize.JSONB(),
                allowNull: true
            }
        }, {
            sequelize,
            timestamps: true,
            modelName: 'Dataset',
            tableName: 'dataset',
            charset: 'utf8'
        })
    }
    // static associate(db) {
    //     db.Dataset.hasMany(db.Theme, {foreignKey: 'theme_ids', targeKey: 'id'})
    // }
}
