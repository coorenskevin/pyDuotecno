# pyDuotecno-ng

`pyDuotecno-ng` is the standalone Python library for communicating with Duotecno controllers.

It contains the protocol and transport layer used by the Home Assistant Duotecno integration, including:

- TCP connection management
- controller login
- packet encoding and decoding
- node and unit discovery
- state updates and command dispatch
- JSON-based startup loading with bus-scan fallback

## Scope

This repository is the backend library only. It is intentionally separate from the Home Assistant integration so the protocol code can be versioned, tested, and released independently.

If you are looking for the Home Assistant integration wrapper, use the companion integration repository that depends on this package.

## Repository Layout

- `controller.py`: main connection and scan orchestration
- `node.py`: node model and unit loading
- `unit.py`: unit classes and device behavior
- `protocol.py`: packet/message definitions and parsing helpers
- `exceptions.py`: backend-specific exceptions

## Intended Usage

The main consumer of this library is the Duotecno Home Assistant integration. The typical flow is:

1. create a controller
2. connect to the Duotecno gateway
3. load nodes and units
4. subscribe to live state updates
5. send commands to supported unit types

## Status

This library is being maintained as a standalone public backend so it can be consumed cleanly by the Home Assistant integration and other future tooling if needed.

## Releases

This repository is intended to publish tagged releases that match the PyPI package version exactly.

The PyPI distribution name is `pyDuotecno-ng`. The Python import package remains `duotecno`.

Release flow:

1. update `pyproject.toml` to the new version
2. commit the version change to `main`
3. create and push a matching tag in the form `vYYYY.M.N`
4. GitHub Actions builds the wheel and source distribution
5. the workflow publishes the package to PyPI
6. the same release artifacts are attached to the GitHub release

This matches Home Assistant expectations that:

- the dependency source is public
- published package versions correspond to tagged source releases
- PyPI artifacts are built from the same tagged revision

## License

This project is licensed under the Apache License 2.0. See [`LICENSE`](LICENSE).
