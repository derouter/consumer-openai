# DeRouter OpenAI consumer

[![Build](https://github.com/derouter/consumer-openai/actions/workflows/build.yaml/badge.svg)](https://github.com/derouter/consumer-openai/actions/workflows/build.yaml)

A [DeRouter](https://derouter.org) consumer module conforming the [OpenAI protocol](https://github.com/derouter/protocol-openai).

It exposes a local OpenAI-compatible server which routes requests to the DeRouter network.
It requies a running [DeRouter node](https://github.com/derouter/derouter) to connect to.
See [`config.example.jsonc`](./config.example.jsonc) for example configuration.

## 👷 Development

```sh
npm run build
npm start -- -c./config.json
```

```sh
# Run with nodemon.
npm run dev -- -- -- -c./config.json
```

## 🚀 Releases

The module may be compiled into a single binary with NodeJS's [SEA](https://nodejs.org/api/single-executable-applications.html).
You can download a binary from the [Releases](https://github.com/derouter/consumer-openai/releases) page, or build it manually.

### Linux

```sh
./scripts/compile/linux.sh
./dist/bin/output -c./config.json
```

### Windows

```pwsh
./scripts/compile/win32.ps1
./dist/bin/output.exe -c ./config.json
```

### MacOS

```sh
./scripts/compile/darwin.sh
./dist/bin/output -c./config.json
```
