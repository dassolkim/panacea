const rp = require('./rdf-parser')
const fh = require('../fileHandler/file-handler')

const formatList = ['CSV', 'GeoJSON', 'KML', 'JSON', 'HTML', 'XLSX']


function test(){
    const format = 'JSON'
    const sourceInfo = {
        type: 'catalog',
        publisher: 'US',
        name: 'us_catalog'
    }
    const catalog = rp.catalogParser(fh.readCatalog('./data/', sourceInfo))
    const datasets = rp.datasetParser(catalog)
    const urls = rp.distributionParser(catalog, format)
    const schema = rp.schemaParser(catalog)
    //const dist = test['rdf:RDF']['dcat:Distribution']
    // console.log(JSON.stringify(catalog, null, 2))
    // console.log(catalog)
    console.log(schema)
    const url_count = urls.length
    const dataset_count = datasets.length
    console.log(`number of ${format} files in catalog: ${url_count}`)
    console.log(`${format} urls in catalog: ${urls}`)
    console.log(`Number of datasets in catalog: ${dataset_count}`)

}
test()

// UK catalog example
const uk_catalog_data = {
    '?xml': { '@_version': '1.0', '@_encoding': 'utf-8' },
    'rdf:RDF': {
      'dcat:Catalog': {
        'dcat:dataset': [Array],
        'foaf:homepage': [Object],
        'dct:language': 'en_GB',
        'dct:description': 'Data publisher',
        'dct:title': 'data.gov.uk',
        'dct:modified': [Object],
        '@_rdf:about': 'https://ckan.publishing.service.gov.uk'
      },
      'hydra:PagedCollection': {
        'hydra:itemsPerPage': [Object],
        'hydra:firstPage': 'https://ckan.publishing.service.gov.uk/catalog.rdf?page=1',
        'hydra:nextPage': 'https://ckan.publishing.service.gov.uk/catalog.rdf?page=2',
        'hydra:totalItems': [Object],
        'hydra:lastPage': 'https://ckan.publishing.service.gov.uk/catalog.rdf?page=520',
        '@_rdf:about': 'https://ckan.publishing.service.gov.uk/catalog.rdf?page=1'
      },
      'dcat:Distribution': [
        [Object], [Object], [Object], [Object], [Object], [Object],
        [Object], [Object], [Object], [Object], [Object], [Object]
        //... 415 more items
      ],
      'foaf:Organization': [
        [Object], [Object],
        [Object], [Object],
        [Object], [Object],
        [Object], [Object],
        [Object]
      ],
      '@_xmlns:hydra': 'http://www.w3.org/ns/hydra/core#',
      '@_xmlns:dcat': 'http://www.w3.org/ns/dcat#',
      '@_xmlns:dct': 'http://purl.org/dc/terms/',
      '@_xmlns:foaf': 'http://xmlns.com/foaf/0.1/',
      '@_xmlns:locn': 'http://www.w3.org/ns/locn#',
      '@_xmlns:rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
    }
  }