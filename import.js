var { Readable } = require( 'stream' );
var logger = require( 'pelias-logger' ).get( 'pelias-VRK' );
var DocumentStream = require('./documentStream');
var AdminLookupStream = require('pelias-wof-admin-lookup');
var model = require( 'pelias-model' );
var peliasDbclient = require( 'pelias-dbclient' );
var { Client } = require('@elastic/elasticsearch');

/**
 * Import DVV addresses from the Ryhti open_address OGC API Features collection
 * into Pelias elasticsearch.
 */

const API_URL = process.env.DVV_API_URL ||
  'https://paikkatiedot.ymparisto.fi/geoserver/ryhti_building/ogc/features/v1/collections/open_address/items';
const PAGE_SIZE = 20000;
const MAX_RETRIES = 5;

const hashes = {};

// read all existing address documents from ES into a hashtable for deduping
async function createDeduper() {
  var hashCount = 0;

  const client = new Client({ node: 'http://localhost:9200' });

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

  responseQueue.push(response.body);

  while (responseQueue.length) {
    const body = responseQueue.shift();
    body.hits.hits.forEach(addHash);

    // check to see if we have collected all docs
    if (body.hits.total.value === hashCount) {
      logger.info('Extracted ' + hashCount + ' existing addresses');
      break;
    }
    // get the next response if there are more items
    const next = await client.scroll({
      scroll_id: body._scroll_id,
      scroll: '30s'
    });
    responseQueue.push(next.body);
  }
}

async function fetchPage(startIndex) {
  // sortBy keeps paging stable
  const url = API_URL + '?f=application/json&sortBy=address_key&limit=' + PAGE_SIZE +
    '&startIndex=' + startIndex;
  for (let attempt = 1; ; attempt++) {
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(300000) });
      if (!response.ok) {
        throw new Error('HTTP ' + response.status);
      }
      return await response.json();
    } catch (err) {
      if (attempt >= MAX_RETRIES) {
        throw err;
      }
      logger.warn('Fetching ' + url + ' failed (' + err.message + '), retrying');
      await new Promise(resolve => setTimeout(resolve, attempt * 5000));
    }
  }
}

// yields address records one page at a time
async function* readAddresses() {
  let startIndex = 0;
  let total;
  while (total === undefined || startIndex < total) {
    const page = await fetchPage(startIndex);
    total = page.numberMatched;
    if (!Number.isInteger(total)) {
      throw new Error('API response has no numberMatched');
    }
    if (!page.features.length) {
      // a partial address set must not be accepted as a successful import
      throw new Error('API returned no features at ' + startIndex + ' / ' + total);
    }
    logger.info('Fetched addresses ' + startIndex + '-' + (startIndex + page.features.length) + ' / ' + total);
    startIndex += page.features.length;
    for (const feature of page.features) {
      yield { properties: feature.properties, coordinates: feature.geometry && feature.geometry.coordinates };
    }
  }
}

// Stream for indexing dvv data into elasticsearch
function createImportPipeline() {
  logger.info( 'Importing addresses from ' + API_URL );

  createDeduper().then(() =>  {
    var documentStream = DocumentStream.create(hashes);
    var adminLookupStream = AdminLookupStream.create();
    var finalStream = peliasDbclient({});
    Readable.from( readAddresses() )
      .on( 'error', err => { logger.error( err ); process.exit( 1 ); } )
      .pipe( documentStream )
      .pipe( adminLookupStream )
      .pipe( model.createDocumentMapperStream() )
      .pipe( finalStream );
  });
}

// run import
createImportPipeline();
