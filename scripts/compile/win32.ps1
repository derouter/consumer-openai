Write-Host 'Hello, World!'

$ErrorActionPreference = "Stop"

$BIN = "dist/bin/output.exe"

npx esbuild `
    --format=cjs `
    --platform=node `
    --bundle `
    --tsconfig=tsconfig.json `
    --out-extension:.js=.js `
    --outfile=dist/bundle/main.js `
    src/index.ts

New-Item -ItemType Directory -Force -Path "dist/bin" | Out-Null

node --experimental-sea-config sea-config.json

node -e "require('fs').copyFileSync(process.execPath, '$BIN')"

npx postject $BIN NODE_SEA_BLOB dist/bin/sea-prep.blob `
    --sentinel-fuse NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2
