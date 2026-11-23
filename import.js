var { Readable } = require( 'stream' );
var fs = require( 'fs' );
var zlib = require( 'zlib' );
var readline = require( 'readline' );
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
// Syke asks long-term users to identify themselves with this parameter
const SYKE_USER_ID = process.env.SYKE_USER_ID || 'hsldigitransit';
const PAGE_SIZE = 20000;
const MAX_RETRIES = 5;
// While fetching from the API, also save the addresses into this gzipped NDJSON file
const DUMP_FILE = process.env.DVV_DUMP_FILE;
// Import from a file written with DVV_DUMP_FILE instead of fetching from the API
const SOURCE_FILE = process.env.DVV_SOURCE_FILE;

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
    '&startIndex=' + startIndex + '&sykeuserid=' + encodeURIComponent(SYKE_USER_ID);
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
async function* fetchAddresses() {
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

// The last line of a dump has the address count, so that a truncated dump can be detected.
async function* readDump(file) {
  logger.info('Reading addresses from ' + file);
  const lines = readline.createInterface({
    input: fs.createReadStream(file).pipe(zlib.createGunzip()),
    crlfDelay: Infinity
  });
  let count = 0;
  let expected;
  for await (const line of lines) {
    if (!line) {
      continue;
    }
    const record = JSON.parse(line);
    if (record.properties) {
      count++;
      yield record;
    } else {
      expected = record.count;
    }
  }
  if (count !== expected) {
    throw new Error('Address dump is incomplete: ' + count + ' / ' + expected);
  }
}

// Passes the addresses through and saves them into file. The file is complete only when
// the source has been read to the end, so it is written under a temporary name first.
async function* saveDump(addresses, file) {
  const tmpFile = file + '.tmp';
  const out = zlib.createGzip();
  const fileDone = new Promise((resolve, reject) => {
    const target = fs.createWriteStream(tmpFile);
    target.on('error', reject);
    target.on('finish', resolve);
    out.on('error', reject);
    out.pipe(target);
  });
  fileDone.catch(() => {});
  const write = async line => {
    if (!out.write(line + '\n')) {
      await new Promise(resolve => out.once('drain', resolve));
    }
  };
  let count = 0;
  try {
    for await (const address of addresses) {
      count++;
      await write(JSON.stringify(address));
      yield address;
    }
    await write(JSON.stringify({ count }));
    out.end();
    await fileDone;
    fs.renameSync(tmpFile, file);
    logger.info('Saved ' + count + ' addresses to ' + file);
  } catch (err) {
    out.destroy();
    fs.rmSync(tmpFile, { force: true });
    throw err;
  }
}

function readAddresses() {
  if (SOURCE_FILE) {
    return readDump(SOURCE_FILE);
  }
  return DUMP_FILE ? saveDump(fetchAddresses(), DUMP_FILE) : fetchAddresses();
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
