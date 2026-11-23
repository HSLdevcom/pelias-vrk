# DVV (formerly VRK) import pipeline

A tool for importing DVV address data into Pelias. Data is read directly from the Ryhti `open_address` OGC API Features collection (Syke), which is updated daily.

## Install dependencies

```bash
yarn install
```

## Usage

`node import.js`: fetch all addresses from the API and import them.

To keep a backup of the source data, set `DVV_DUMP_FILE=<path>.gz`: the fetched addresses are also saved to a gzipped NDJSON file.
The file is written under a temporary name and renamed only after all addresses were fetched, so an incomplete fetch never leaves a dump behind.
If the API is unavailable, set `DVV_SOURCE_FILE=<path>.gz` to import from such a file instead of the API. Truncated dumps are rejected.

The API URL can be overridden with the `DVV_API_URL` environment variable.
Requests carry the Syke user identifier as `sykeuserid` (default `hsldigitransit`, override with `SYKE_USER_ID`).
Syke asks long-term or heavy users to request an application identifier from gistuki@syke.fi.

Document ids are the `address_key` of the address; ids in `imports.blacklist` are matched against it.
