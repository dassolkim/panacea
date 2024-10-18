const Sequelize = require('sequelize')

module.exports = class Theme extends Sequelize.Model {
    static init(sequelize) {
        return super.init({
            id: {
                type: Sequelize.UUID,
                allowNull: false,
                primaryKey: true
            },
            title: {
                type: Sequelize.STRING(100),
                allowNull: false,
                unique: true
            },
            taxonomy: {
                type: Sequelize.STRING(20),
                allowNull: true,
                unique: true
            }
        }, {
            sequelize,
            timestamps: true,
            modelName: 'Theme',
            tableName: 'theme',
            charset: 'utf8'
        })
    }
    // static associate(db) {
    //     db.Format.belongsTo(db.DatasetFormat, {foreignKey: 'format_id', targeKey: 'id'})
    // }
}
