const create = require('./action/create')

const dataset = {
    url: "test____1",
    title: "test",
    issued: "2022-10-31 22:25:49.811+09",
    modified: "2022-10-31 22:25:49.811+09",
    identifier: "test",
    publisher: "test_publisher"
}

create.dataset_create(dataset)