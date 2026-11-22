# Shopping Sessions — Rust pipeline


A complete, correct-first implementation of `rules.md` in Rust. It passes all 36
edge-case fixtures and matches an independent Python reference exactly on
synthetic dirty data. Run `make check-dev` against the published dev answer
before your first submission (see "Before you submit").


## Why Rust

The score is wall-clock time from container start to exit, including engine
startup and CSV reading, and correctness is all-or-nothing. A single static
binary starts in milliseconds, and a hand-written parser implements the CSV
dialect exactly. Generic CSV readers (Polars, DuckDB, Spark) each have their own
ideas about quotes, `\r` and malformed lines, which rules.md overrides.

## Layout

```
Dockerfile            two-stage build: rust:1.85 builder -> debian slim runtime, runs as uid 1000
Cargo.toml/.lock      memchr, memmap2, xxhash-rust, parquet (no Arrow, no codecs)
src/csv.rs            the CSV dialect, exactly (quotes, \r, field counts, empty lines)
src/ts.rs             timestamp forms F1/F2/F3, calendar and offset validation, ASCII digits only
src/out.rs            Parquet writer (day = INT32 DATE, strings UTF8)
src/main.rs           the pipeline
scripts/reference.py  slow, literal Python implementation of rules.md, for differential testing
scripts/synth.py      synthetic dirty-data generator exercising every dirt type in rules.md
scripts/run_fixtures.py, compare.py, docker_run.sh   from the starter
fixtures/             the 36 edge cases (excluded from the Docker build context)
rules.md              the spec; it wins over everything else
```

## How the pipeline works

Everything stays in memory: about 5 GB of CSV and well under 24 GB of working
set at final size. One pool of `$NCPU` threads runs each stage:

1. **Small inputs.** manifest, products, campaigns. Categories are sorted
   byte-wise (with `unknown` added), so "tie goes to the smallest category"
   becomes "smallest integer id".
2. **Links.** Parsed in parallel, then union-find, with each component named by its
   byte-wise smallest ID. A person's *key* is `xxh3_64(name)`. An unlinked ID's
   key is just the hash of the ID, so only linked IDs whose name is a *different*
   ID need a lookup entry.
3. **Parse events and clicks.** The files are memory-mapped and split into ~8 MB
   chunks at line boundaries, largest first. Each line is checked against the
   dialect. Validation and the late check depend only on that copy, so they are
   decided here and stored as a flag. Event copies go into 1,024 buckets by
   `xxh3_128(event_id)`. Valid clicks go into 1,024 buckets by person key.
4. **Dedup.** For each event-id bucket, a hash map keeps the copy with the
   smallest `ingest_ts`. That copy survives only if its own flag says valid, so
   dedup happens before validation and a later valid copy never rescues the event.
   Survivors are scattered into person buckets.
5. **Sessionize and attribute.** For each person bucket, sort events by
   `(person, ts)` and clicks by `(person, ts, click_id descending)`, then walk each
   person:
   * A session breaks when `ts - prev > 30 min`. Equal timestamps have a gap of
     0, so they always join.
   * Two pointers keep the click window `[start - 7d, start]` with a multiset of
     its campaigns. The credit is the last click in the window: the latest `ts`
     and, thanks to the descending sort, the smallest `click_id`. The assists are
     the campaigns currently in the multiset.
   * user_daily is aggregated per (person, day of session start).
   * campaign_daily is aggregated into a dense `[campaign][day]` array per thread.
6. **Write.** One user_daily file per worker, written in parallel, plus one
   campaign_daily file.

Progress and timings are logged to stderr.

### Assumptions to know about

* **Hashes as identity.** Event IDs are deduplicated by 128-bit xxh3, so a
  collision is practically impossible. Person keys and the link lookup use 64-bit
  xxh3, which gives roughly a 1-in-a-million chance of merging two people at
  final size. Product and campaign lookups compare the actual bytes.
* **No click dedup.** rules.md guarantees that copies of a click have identical
  values. The credit picks one click and the assists count each campaign once,
  so duplicate copies cannot change any output. The reference does dedup, and
  the two agree.
* An `ingest_ts` that fails to parse (rules.md says it never does) is treated as
  +infinity.

## Local workflow

You need cargo (Rust 1.85 or newer) and `pip install polars==2.1.0` for the test
scripts. Docker is only needed for the scorer-like targets.

```sh
make test            # unit tests: CSV dialect + timestamps
make fixtures-local  # 36/36 fixtures, native binary, about a second
make synth-check     # Rust vs Python reference on 6 synthetic seeds (odd/even toggle \r dirt)
make synth-check SEEDS="11 12 13" SYNTH_EVENTS=300000   # more/bigger

make build fixtures  # same fixtures through the real image and scorer flags
make dev             # needs `generate` from generator.zip on PATH
make run-dev         # image on dev/ with scorer limits, timed
make check-dev       # exact diff vs dev-answer/ (unzip dev-answer.zip in the repo root)
```

`synth.py` imitates the dirt in rules.md (duplicate events where the earliest
copy has a bad `ts` or a different user, link chains, shared devices, boundary
clicks, same-millisecond ties, quoting, malformed lines, `\r`). It is not the
official generator, so `check-dev` remains the ground truth. The two test
oracles are deliberately independent. When they disagree, read rules.md and work
out which one is wrong. Once, during development, it was the synthesizer.
