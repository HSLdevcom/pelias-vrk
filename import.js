var fs = require( 'fs' );
var logger = require( 'pelias-logger' ).get( 'pelias-VRK' );
var csvParse = require( 'csv-parse' );
var DocumentStream = require('./documentStream');
var AdminLookupStream = require('pelias-wof-admin-lookup');
var model = require( 'pelias-model' );
var peliasDbclient = require( 'pelias-dbclient' );
var elasticsearch = require('elasticsearch');

/**
 * Import VRK addresses ( a CSV file )  into Pelias elasticsearch.
 */

const hashes = {};

// read all existing address documents from ES into a hashtable for deduping
async function createDeduper() {
  var hashCount = 0;

  const client = new elasticsearch.Client({
    host: 'localhost:9200',
    apiVersion: '7.6',
  });

  function addHash(hit) {
    const doc = hit._source;
    const hash = doc.address_parts.street + doc.address_parts.number + doc.parent.postalcode;
    hashes[hash] = true;
    hashCount++;
  }

  const responseQueue = [];

  logger.info( 'Reading existing addresses for deduping');
  const response = await client.search({
    index: 'pelias',
    scroll: '30s',
    size: 10000,
    body: {
      'query': {
        'term': {
          'layer': {
            'value': 'address',
            'boost': 1.0
          }
        }
      }
    }
  });

  responseQueue.push(response);

  while (responseQueue.length) {
    const body = responseQueue.shift();
    body.hits.hits.forEach(addHash);

    // check to see if we have collected all docs
    if (body.hits.total.value === hashCount) {
      logger.info('Extracted ' + hashCount + ' existing addresses');
      break;
    }
    // get the next response if there are more items
    responseQueue.push(
      await client.scroll({
        scrollId: body._scroll_id,
        scroll: '30s'
      })
    );
  }
}

// Stream for indexing vrk data into elasticsearch
function createImportPipeline( fileName ) {
  var csvOptions = {
    trim: true,
    skip_empty_lines: true,
    relax: true,
    delimiter: ';',
    columns: ['id', 'c2', 'c3', 'c4', 'x', 'y', 'c7', 'street', 'street_sv', 'number', 'postcode', 'c12', 'localadmin', 'c14', 'c15', 'c16']
  };

  logger.info( 'Importing addresses from ' + fileName );

  createDeduper().then(() =>  {
    var csvParser = csvParse(csvOptions);
    var documentStream = DocumentStream.create(hashes);
    var adminLookupStream = AdminLookupStream.create();
    var finalStream = peliasDbclient({});
    var documentReader = function(fileName) {
      fs.createReadStream( fileName )
        .pipe( csvParser )
        .pipe( documentStream )
        .pipe( adminLookupStream )
        .pipe( model.createDocumentMapperStream() )
        .pipe( finalStream );
    }
    documentReader(fileName);
  });
}

// run import
createImportPipeline(process.argv[2]);
