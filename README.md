# PulseDB

PulseDB is my attempt at a RDBMS. It is written in C++20.

### Current

- Pages are fixed-sized to 4KB.
- B+ tree indexing system.
- Slotted page storage for variable-length records.

### In Development

- Buffer pool manager for improved I/O performance.
- B+ tree manager for coordinating tree operations.

## Building (nix)

### Build Steps

```bash
nix develop .#pulse-dev --impure --command build-pulsedb
```

### Running Tests

```bash
nix develop .#pulse-dev --impure --command test-pulsedb
```
