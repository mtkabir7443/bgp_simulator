# Routing model

[Back to README](../README.md)

The simulator studies static route selection under commercial routing policy.
It does not implement the complete [BGP protocol (RFC 4271)](https://www.rfc-editor.org/rfc/rfc4271.html)
or full [Route Origin Validation (RFC 6811)](https://www.rfc-editor.org/rfc/rfc6811.html).

## Selection and propagation

- Routes travel upward to providers, across at most one peer link, then downward
  to customers. Provider/customer relationships must form an acyclic hierarchy;
  both engines reject cycles. Peer links may form cycles.
- Selection prefers local origin, then customer, peer, and provider routes.
  Ties use shorter AS paths, then the lower next-hop ASN.
- Paths include the observing AS and are limited to **16 ASNs**. Routes exceeding
  that limit are omitted. This is an implementation limit, not a BGP limit.
- ROV uses a supplied invalid flag and a set of filtering ASes. Invalid routes
  are rejected at those ASes, including locally seeded announcements. The engine
  does not validate ROAs or implement the full Valid/Invalid/NotFound state model.
- Prefixes are matched by input strings. Supply canonical IPv4 CIDRs and origins
  present in the relationship file; unknown origins are currently ignored.
  Conflicting duplicate announcements are not a supported contract.

The model does not simulate sessions, update timers, withdrawals, or convergence
over time. The exploratory prefix-lookup helper is separate from the engine.

## Input and output

Relationship rows use `provider|customer|-1` or `peer|peer|0`:

```text
1|2|0
2|3|-1
```

Announcement rows use `origin_asn,prefix[,rov_invalid]`. The optional third field
is a boolean flag, not a timestamp. Use `0` for valid and `1` for invalid:

```text
1,192.0.2.0/24,0
```

An optional ROV file contains one filtering ASN per line. Pass it using
`--rov-asns <path>`; omit the option when you do not have this file.

```bash
build/bgp_simulator --relationships <relationships-file> --announcements <announcements-file>
build/bgp_sim_gpu --relationships <relationships-file> --announcements <announcements-file>
```

Both engines write `ribs.csv` in the current working directory, replacing an
existing file with that name. Use a separate directory for each run. The inputs
above produce:

```csv
asn,prefix,as_path
1,192.0.2.0/24,"(1,)"
2,192.0.2.0/24,"(2, 1)"
3,192.0.2.0/24,"(3, 2, 1)"
```

Row order is not part of the interface. Compare route contents rather than bytes.

Missing requested files and provider/customer cycles produce diagnostics and a
nonzero CLI exit status; Python calls raise an exception. Readable empty input is
allowed. Strict shared input validation and comprehensive resource error handling
remain unfinished; see [development limits](development.md#architecture-and-resource-limits).

## Python API

Build with `make python` in the activated virtual environment. The extension
lives in `build/`; from the repository root, add it to the import path:

```bash
PYTHONPATH="$PWD/build" python
```

Then use the same file interface:

```python
import bgp_simulator

bgp_simulator.run(
    relationships="data/generated/rel.txt",
    announcements="data/generated/ann.txt",
    rov_asns="",
)
```

This example writes `ribs.csv` in the repository root. To keep results under
`outputs/`, change to a run directory before calling the engine and supply
absolute input paths. Calls are sequential and reset route state between runs.
The binding uses global engine state and writes to the process's current
directory; it is not a concurrent simulation API.
