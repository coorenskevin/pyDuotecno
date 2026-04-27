# pyDuotecno

`pyDuotecno` is the standalone Python library for communicating with Duotecno controllers.

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

## License

This project is licensed under the Apache License 2.0. See [`LICENSE`](LICENSE).
