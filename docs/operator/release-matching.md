# Release matching: operator research guide

Evidence checked on 2026-10-11. `nntp-proxy` transports NNTP and manages routing,
availability, and optional article caching. It does not currently host a PreDB
or release matcher. The matching workflow below is an engineering proposal for
a separate consumer/indexer; NNTmux is a reference consumer, not a component
changed by this work. This guide adds no ingestion, export API, or production
configuration.

## Sources and access boundaries

Prefer a permitted bulk snapshot for local history, then documented feeds for
updates and lookup APIs for individual unresolved candidates. Scene databases
do not imply comprehensive P2P coverage. Public accessibility alone does not
establish permission to clone a service.

| Source | Verified practical use | Limits and uncertainty |
| --- | --- | --- |
| [Skwirll](https://skwirll.com/api) | Monthly SQL dump; live database reports approximately 13.5 million entries. Exact lookup, date lookup, and WebSocket updates; REST limit 30 requests/minute. | Operator explicitly recommends dumps over API cloning. Replay and retention guarantees are undocumented. |
| [PreDB.net](https://predb.net/api-documentation/) | Release lookup, size/files, category, group, nuke information, and NFO references. | 30 requests/minute; page cap 100. Internal IDs can change. Documentation prohibits using the API to copy its database. |
| [srrDB](https://api.srrdb.com/v1/docs) | Release details and searches by stored filename, CRC, archive size/hash, and IMDb. [RSS](https://www.srrdb.com/feed) lists new SRRs and titles. | Documentation says not to scrape. Anonymous `/open` redirected home; approved bulk access remains unverified. Upload timestamps are not pre timestamps. |
| [xREL](https://www.xrel.to/wiki/1681/API.html) | Documented scene and P2P methods, search, and web monthly archives. | Published API limits: 900/hour; search 2/5 seconds. Archives are browsing surfaces, not a verified export. Coverage is incomplete. |
| [PreDB.me](https://predb.me/?rss=1) | Official RSS returned current release names and post GUIDs. | Sample items lacked timestamps, sizes, and nukes; no bulk route verified. |
| [NGP.re](https://ngp.re/?source=p2p) | Current scene/P2P views include FLUX, GeneMige, and AnoZu names. | [Terms](https://ngp.re/terms-of-services) require prior administrator permission for scraping/crawling. Integration is permission-dependent. |
| [M2V](https://m2v.ru/) | Current human-readable release names, groups, sizes, files, and dates. | No documented API, dump, or automation permission established. |
| [Corrupt-Net](https://pre.corrupt-net.org/live.php) | Current live web metadata; published IRC `irc.corrupt-net.org`, TLS 6697/7000, `#Pre`, `#Pre.Spam`, `#Pre.Search`. | IRC events and permission for recording bots were not tested. |
| [NNTmux IRC guidance](https://github.com/NNTmux/newznab-tmux#irc-pre-channels) | Publishes SynIRC `irc.synirc.net:6697`, TLS, `#PreNNTmux` and `#nZEDbPRE`. | Documentation evidence only; channel availability and bot access remain unverified. |
| [PreDB.org](https://predb.org/about) | Officially links ZeNet `#Pre` as an IRC lead. | Channel availability, recording permission, and independent provenance remain unverified. |
| [nZEDb historical dumps](https://github.com/nZEDb/nZEDbPre_Dumps) | Historical CSV backfill reference. | Repository says discontinued; last observed daily artifact was 2024-05-02. |
| [PreDB.ovh documentation](https://predbdotovh.github.io/pre-api/) | Describes historical API, WebSocket, RSS, and SQL dumps. | Current hostname resolution failed; advertised downloads are not verified usable. |

No current public FTP feed was verified. Treat leaked/historical dumps as
provenance-questionable leads, not trustworthy automatic imports. Overlapping
events across websites do not prove independent sourcing.

## Loading Skwirll safely

[The dump endpoint](https://skwirll.com/api/dump) redirected to the 2026-10-01
snapshot, 303,422,178 compressed bytes. Research inspected only a server-honored
64 KiB range in memory. Its SQL creates `temp_dump`, with unique `name`,
`category`, `group`, nullable `files`, `size`, `imdb_id`, `nuke_status`,
`nuke_status_reason`, `nuke_status_updated_at`, and `created_at`. It sets UTC and
includes `DROP TABLE IF EXISTS temp_dump`.

Proposed ingestion: use a separate staging database and restricted importer;
inspect schema, validate counts and timestamps, then publish a versioned local
snapshot. Keep download URL, snapshot date, checksum, and import outcome.
Reader consumers should use read-only access. Do not execute upstream SQL in
production services.

Observed WebSocket frames use `{type, release}`: `addpre` can have null
size/files, followed by `info` enrichment. Identical updates occurred, so upsert
idempotently by release name. A sampled date endpoint returned 2,304 ascending
rows within the UTC day; generated pagination links dropped the requested
limit. Preserve filters explicitly. REST omitted nuke fields even for a
dump-known nuked release: do not interpret omission as “not nuked.” Gap replay,
correction recovery, nuke event format, and MB-versus-MiB precision remain open.

## Proposed evidence ladder

1. **Exact title and aliases.** Keep the original subject, extracted title,
   filename aliases, source record, and transformation history. Remove only
   recognized posting wrappers: quoted filenames, multipart counters, and
   recognized `NZBSPLIT` delimiters; see NNTmux's [wrapper parser](https://github.com/nntmux/newznab-tmux/blob/dd2be479c44243b5df9be97bdb1ef00a1f97bdac/app/Services/NameFixing/NzbSplitUnwrapper.php).
   Treat extraction as candidate generation;
   arbitrary punctuation removal can erase meaningful distinctions. A filename
   alias needs an explicit association with a release, not a guessed basename.

2. **Known title hashes.** nZEDb's [0368 SQL patch](https://github.com/nZEDb/nZEDb/blob/master/resources/db/patches/mysql/0368~predb_hashes.sql)
   creates `MD5(title)`, `MD5(MD5(title))`, and `SHA1(title)` lookup values. The
   inner MD5 in the double form is hexadecimal text, not raw digest bytes.
   Reproduce exact title bytes, character encoding, and representation; compare
   against unique candidates. These are aliases of names, not content hashes
   or proof of authenticity. Hashing an already damaged title cannot restore it.

3. **Posting identity.** Preserve case-sensitive Message-IDs and compare ordered
   segment mappings or complete sets with segment counts. [RFC 5536](https://www.rfc-editor.org/rfc/rfc5536.html#section-3.1.3)
   defines Message-ID identity by octet comparison. Crossposts sharing IDs can
   identify the same posting across groups; this does not identify its exact
   scene release, edition, or repack. Partial overlap alone is insufficient.

4. **Validated embedded metadata.** A future consumer may obtain yEnc `name=`
   and declared sizes from BODY data, validate decoding/checksums, and use
   bounded PAR2 metadata parsing. [PAR2 File Description packets](https://parchive.github.io/doc/Parity%20Volume%20Set%20Specification%20v2.0.html)
   contain filenames, file length, whole-file MD5, and first-16-KiB MD5. Verify
   packet integrity and lengths before trusting names. A matching prefix hash
   plus length is corroboration, not a substitute for whole-file verification.
   This research retrieved no article payloads.

5. **Comparable checksums.** Match only equal scopes: yEnc part CRC concerns a
   decoded part; full-file CRC concerns an assembled file; SFV describes named
   files. srrDB's [`archive-crc`](https://www.srrdb.com/help) searches files
   inside RARs, while `files[]` in its details response describes outer volumes.
   `store-real-crc` identifies an SRS sample. Keep these scopes separate and
   corroborate candidates with exact lengths and additional manifest entries.
   PAR2 packet checksums and title hashes have different purposes. Record
   algorithm, byte scope, length, and filename alongside every digest.

Keep group, repack/proper, edition, language, source, codec, and resolution as
hard conflict checks. Store competing claims with provenance and confidence;
never automatically merge conflicting releases from a fuzzy title similarity.
Ambiguous candidates should remain unresolved or require review.

## Transport and reader compatibility

PR #12 was squash-merged into `origin/main` at `2f5a28c`. Client
`XFEATURE COMPRESS GZIP`, including its optional `TERMINATOR` form, now
receives `503`; the previous compressed-overview framing hang is guarded by
rejecting that negotiation. In `per-command` mode, `MODE READER` receives
`502` and closes the connection. Its capabilities omit `READER`, `OVER`,
and `HDR`; hybrid and stateful modes retain reader capabilities.
Backend RFC 8054 `COMPRESS DEFLATE` remains a separate setting.

Reader Message-ID failover is opt-in:

```toml
[routing]
reader_message_id_failover = true
```

The default is `false`. In hybrid sessions after group selection, the pinned
backend is tried first for explicit Message-ID `ARTICLE`, `BODY`, `HEAD`,
and `STAT`. Only a completed authoritative `430` enables tier-aware retries;
transport errors are not missing facts. Exhaust eligible providers in a tier
before descending. Group selection, overview ranges, numeric and argumentless
commands retain their backend and article pointer. Stateful mode is unaffected.
See [routing guidance](runtime-and-routing.md).

Documentation alone does not establish live deployment. Confirm the deployed
main revision and routing configuration before depending on this behavior.
