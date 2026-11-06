var through = require( 'through2' );
var peliasModel = require( 'pelias-model' );
var logger = require( 'pelias-logger' ).get( 'pelias-VRK' );
var config = require('pelias-config').generate();

// house number from the address parts, e.g. 41a or 2-26
function formatNumber(p) {
  if (p.number_part_of_address_number == null) {
    return undefined;
  }
  var number = '' + p.number_part_of_address_number + (p.subdivision_letter_of_address_number || '');
  if (p.number_part_of_address_number2 != null) {
    number += '-' + p.number_part_of_address_number2 + (p.subdivision_letter_of_address_number2 || '');
  }
  return number;
}

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
      var p = rec.properties;
      var street = p.address_name_fin || p.address_name_swe;
      var number = formatNumber(p);
      var postcode = p.postal_code;
      if (rec.coordinates && street && number) {
        var hash = street + number + postcode; // dedupe
        var model_id = p.address_key;
        if (!hashes[hash] && !blacklist[model_id]) {
          hashes[hash]=true;
          try {
            var doc = new peliasModel.Document( sourceName, 'address', model_id )
                .setName( 'default', street + ' ' + number )
                .setCentroid( { lon: rec.coordinates[0], lat: rec.coordinates[1] } )
                .setPopularity(10)
                .setAddress( 'street', street )
                .setAddress( 'number', number );

            if(p.address_name_swe && p.address_name_swe !== street) {
              doc.setName( 'sv', p.address_name_swe + ' ' + number );
            }
            if(postcode) {
              try {
                doc.addParent( 'postalcode', postcode, '?' );
                doc.setAddress( 'zip', postcode );
              }
              catch (err) { logger.info('invalid postalcode', err); }
            }
            this.push( doc );
          }
          catch ( ex ){
            logger.error('error', ex);
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
