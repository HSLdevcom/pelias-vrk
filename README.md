# DVV (formerly VRK) import pipeline

A tool for importing DVV address data into Pelias. Data is read directly from the Ryhti `open_address` OGC API Features collection (Syke), which is updated daily.

## Install dependencies

```bash
yarn install
```

## Usage

`node import.js`: fetch all addresses from the API and import them.

The API URL can be overridden with the `DVV_API_URL` environment variable.
Syke asks long-term or heavy users to request an application identifier from gistuki@syke.fi.

Document ids are the `address_key` of the address; ids in `imports.blacklist` are matched against it.
