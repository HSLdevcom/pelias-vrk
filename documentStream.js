var through = require( 'through2' );
var peliasModel = require( 'pelias-model' );
var logger = require( 'pelias-logger' ).get( 'pelias-VRK' );
var proj4 = require('proj4');
var config = require('pelias-config').generate();

proj4.defs('EPSG:3067', '+proj=utm +zone=35 +ellps=GRS80 +units=m +no_defs');

function createDocumentStream(oldHashes) {
  var sourceName = 'openaddresses';
  var deduped = 0;
  var hashes = oldHashes || {};
  var blacklist = {};

  if(config && config.imports && config.imports.blacklist) {
    var bl = config.imports.blacklist;
    if (Array.isArray(bl) && bl.length > 0) {
      bl.forEach(function mapId(id) { blacklist[id] = true; });
    }
  }

  return through.obj(
    function write( rec, enc, next ){
      if (rec.x && rec.y && rec.street && rec.number) {
        var hash = rec.street + rec.number + rec.postcode; // dedupe
	var model_id = rec.id + '-' + rec.c7;
        if (!hashes[hash] && !blacklist[model_id]) {
          hashes[hash]=true;
          try {
            var name = rec.street + ' ' + rec.number;

            var srcCoords = [Number(rec.y), Number(rec.x)];
            var dstCoords = proj4('EPSG:3067', 'WGS84', srcCoords);

            var doc = new peliasModel.Document( sourceName, 'address', model_id )
                .setName( 'default', name )
                .setCentroid( { lon: dstCoords[0], lat: dstCoords[1] } )
                .setPopularity(10)
                .setAddress( 'street', rec.street )
                .setAddress( 'number', rec.number );

            if(rec.street_sv && rec.street_sv !== rec.street) {
              doc.setName( 'sv', rec.street_sv + ' ' + rec.number );
            }
            if(rec.postcode) {
              try {
                doc.addParent( 'postalcode', rec.postcode, '?' );
                doc.setAddress( 'zip', rec.postcode );
              }
              catch (err) { logger.info('invalid postalcode', err); }
            }
            if(rec.locality) {
              try { doc.addParent( 'locality', rec.locality, '?' ); }
              catch (err) { logger.info('invalid locality', err); }
            }
            this.push( doc );
          }
          catch ( ex ){
            logger.error('error');
          }
        } else {
          deduped++;
        }
      }
      next();
    }, function end( done ) {
      done();
      logger.info('deduped = ', deduped);
    }
  );
}

module.exports = {
  create: createDocumentStream
};
