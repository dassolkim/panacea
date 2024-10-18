const Sequelize = require('sequelize')

module.exports = class Format extends Sequelize.Model {
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
            },
            domain: {
                type: Sequelize.STRING(50),
                allowNull: false
            }
        }, {
            sequelize,
            timestamps: true,
            modelName: 'Format',
            tableName: 'format',
            charset: 'utf8'
        })
    }
    // static associate(db) {
    //     db.Format.belongsTo(db.DatasetFormat, {foreignKey: 'format_id', targeKey: 'id'})
    // }
}
