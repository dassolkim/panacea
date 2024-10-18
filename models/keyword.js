const Sequelize = require('sequelize')

module.exports = class Keyword extends Sequelize.Model {
    static init(sequelize) {
        return super.init({
            id: {
                type: Sequelize.UUID,
                allowNull: false,
                primaryKey: true
            },
            title: {
                type: Sequelize.STRING(20),
                allowNull: false,
                unique: true
            }
        }, {
            sequelize,
            timestamps: true,
            modelName: 'Keyword',
            tableName: 'keyword',
            charset: 'utf8'
        })
    }
    // static associate(db) {
    //     db.Keyword.belongsTo(db.DatasetKeyword, {foreignKey: 'keyword_id', targeKey: 'id'})
    // }
}
