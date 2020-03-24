var fs = require( 'fs' );
var logger = require( 'pelias-logger' ).get( 'pelias-VRK' );
var csvParse = require( 'csv-parse' );
var DocumentStream = require('./documentStream');
var AdminLookupStream = require('pelias-wof-admin-lookup');
var model = require( 'pelias-model' );
var peliasDbclient = require( 'pelias-dbclient' );

/**
 * Import VRK addresses ( a CSV file )  into Pelias elasticsearch.
 */

function createImportPipeline( fileName ) {
  var csvOptions = {
    trim: true,
    skip_empty_lines: true,
    relax: true,
    delimiter: ';',
    columns: ['id', 'c2', 'c3', 'c4', 'x', 'y', 'c7', 'street', 'street_sv', 'number', 'postcode', 'c12', 'localadmin', 'c14', 'c15', 'c16']
  };

  logger.info( 'Importing addresses from ' + fileName );

  var csvParser = csvParse(csvOptions);
  var documentStream = DocumentStream.create();
  var adminLookupStream = AdminLookupStream.create();
  var finalStream = peliasDbclient({});

  var documentReader = function(fileName) {
    fs.createReadStream( fileName )
      .pipe( csvParser )
      .pipe( documentStream )
      .pipe( adminLookupStream )
      .pipe( model.createDocumentMapperStream() )
      .pipe( finalStream );
  };

  documentReader(fileName);
}

module.exports = {
  create: createImportPipeline
};
