var through = require( 'through2' );
var peliasModel = require( 'pelias-model' );
var proj4 = require('proj4');

proj4.defs('EPSG:3067', '+proj=utm +zone=35 +ellps=GRS80 +units=m +no_defs');

var hashes = {};

function createDocumentStream() {
  var sourceName = 'openaddresses';
  var deduped = 0;

  return through.obj(
    function write( rec, enc, next ){
      if (rec.x && rec.y && rec.street && rec.number) {
        var hash = rec.street + rec.number + rec.postcode; // dedupe
        if (!hashes[hash]) {
          hashes[hash]=true;
          try {
            var model_id = rec.id + '-' + rec.c7;
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
            this.push( doc );
          }
          catch ( ex ){
            console.log('error');
          }
        } else {
          deduped++;
        }
      }
      next();
    }, function end( done ) {
      done();
      console.log('deduped = ', deduped);
    }
  );
}

module.exports = {
  create: createDocumentStream
};
