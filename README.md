# DeRouter OpenAI consumer

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

## ⚒️ Compilation

The module may be compiled into a single binary with NodeJS's [SEA](https://nodejs.org/api/single-executable-applications.html).
This feature is currently only available on MacOS.

```sh
./compile.sh
./dist/bin/main -c./config.json
```
