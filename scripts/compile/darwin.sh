#!/bin/sh
set -e

BIN="dist/bin/output"

npx esbuild \
    --format=cjs \
    --platform=node \
    --bundle \
    --tsconfig=tsconfig.json \
    --out-extension:.js=.js \
    --outfile=dist/bundle/main.js \
    src/index.ts

mkdir -p dist/bin

node --experimental-sea-config sea-config.json

cp $(command -v node) $BIN

codesign --remove-signature $BIN

npx postject $BIN NODE_SEA_BLOB dist/bin/sea-prep.blob \
    --sentinel-fuse NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2 \
    --macho-segment-name NODE_SEA

codesign --sign - $BIN
