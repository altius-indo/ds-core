# GQL parser approach

- **Task:** TASK-0015, parser half (STORY-0010; feeds TASK-0016, TASK-0018, TASK-0019, TASK-0020)
- **Status:** Draft — pending review
- **Satisfies:** REQ-0003 (conformance, feature-named errors), REQ-0004
- **Bound by:**
  - DEC-0005: GQL (ISO/IEC 39075:2024) is the only language.
  - DEC-0006: DELETE and DETACH DELETE semantics.
  - DEC-0007: the graph is schemaless.
  - DEC-0011: pure Rust; native code only from the allow-list.

> **Verification note.** Written without web access. Statements about the opengql
> grammar repo, antlr4rust, pest, lalrpop, chumsky and tree-sitter are background
> knowledge marked **[verify]**.
>
> The other half of TASK-0015, `docs/gql-conformance.toml` (one row per ISO/IEC 39075
> feature), is **not yet written**. Its feature IDs must come from a verified source,
> not from memory.

## 1. What the parser must do

1. **Accept the v1 subset of GQL** from the conformance matrix: id-keyed MATCH, SET
   and INSERT (REQ-0022); one-hop and multi-hop patterns (REQ-0023); DELETE and DETACH
   DELETE (DEC-0006); index DDL (REQ-0031).
2. **Reject everything else with a *feature-named* error** (REQ-0003 AC3), for example:
   `GQL feature GP05 (…) is not supported in this release`.
   - This needs the parser to **recognise** unsupported constructs well enough to name
     them, not just fail on them.
   - STORY-0010's E3 also requires SQL input (for example `SELECT … FROM`) to be
     rejected with a clear "not GQL" error.
3. **Keep latency off the OLTP path.** Point and one-hop queries have tight latency
   budgets (REQ-0022/0023), so parse plus bind must be in the tens of microseconds.
   A plan cache keyed by normalised text helps, but parsing should be cheap anyway.
4. **Be pure Rust** (DEC-0011).

## 2. Options

| Option | Error quality | Fit to the spec grammar | Maintenance | Pure Rust | Notes |
|---|---|---|---|---|---|
| **Hand-written recursive descent + Pratt expressions** | Best: every production can emit a precise, feature-tagged error and recover | Manual transcription of the BNF; drift is possible | Moderate. The grammar is big, but v1 implements a subset and the rest only needs *recognising* | Yes | What most production SQL engines with good errors end up with **[verify examples]** |
| **ANTLR (opengql grammar) + antlr4rust** | Generic "mismatched input" errors; feature naming needs a separate tree walk | Highest: the grammar is mechanically derived from the standard **[verify licence and completeness]** | Generated code plus a runtime crate that is less mature than the Java/C++ targets **[verify]** | Yes **[verify]** | Big generated parser, slower; adaptive prediction costs at runtime |
| **pest (PEG)** | Fair; custom errors are awkward | PEG ordered choice hides ambiguities in a grammar written as BNF | Grammar file plus a hand-built AST layer | Yes | Fast to prototype |
| **lalrpop (LR(1))** | Fair; LR errors are hard to make friendly | The full GQL grammar is unlikely to be LR(1) without heavy rewrites | Grammar conflicts are painful | Yes | — |
| **chumsky (combinators)** | Good, with recovery built in | Manual, like hand-written | Compile times and type complexity on a grammar this size **[verify]** | Yes | — |
| **tree-sitter** | Good recovery; built for editors | A grammar exists for some GQL dialects **[verify]** | — | **No**: C runtime, so not on the DEC-0011 allow-list | Ruled out unless the allow-list grows |

## 3. Recommendation

**Hand-written recursive-descent parser with Pratt parsing for expressions, checked
against the opengql grammar in CI.**

- **Errors are the requirement.** The hardest requirement here is REQ-0003 AC3. A
  hand-written parser can name the GQL feature at the exact point it meets an
  unsupported construct.
- **Recognise, then reject.** For each feature marked `unsupported` in
  `gql-conformance.toml`, the parser has a small *recognise-and-reject* production
  that consumes the construct's leading tokens and raises `Unsupported { feature_id }`.
  This grows the parser one feature at a time; moving a feature to `supported` means
  replacing its reject stub with a real production.
- **Drift control.**
  - **Keyword test:** a dev-only test in `tests/gql_grammar_drift.rs` reads the
    opengql grammar's keyword and token lists and fails if the lexer is missing any
    reserved word.
  - **Corpus test:** `tests/gql_conformance.rs` holds a corpus of example queries per
    feature. Supported features must parse; unsupported ones must produce *their*
    feature ID.
  - ANTLR is used only as a test-time oracle, never at runtime. No Java or ANTLR runtime
    ships in the server.
- **Performance.** Hand-written parsers allocate little and work over one token
  vector, so microsecond-scale parsing of point queries is realistic.

## 4. Pipeline boundary (for TASK-0016)

```
text ─▶ lexer ─▶ parser ─▶ AST ─▶ binder ─▶ logical plan ─▶ planner ─▶ physical plan ─▶ executor
        (gql/lexer.rs) (gql/parser.rs) (gql/ast.rs) (gql/bind.rs) (gql/plan/)        (gql/exec/)
```

**Lexer:**
- Produces a `Vec<Token>` with byte spans.
- Keywords are case-insensitive.
- Delimited identifiers and string escapes follow ISO/IEC 39075.
- SQL detection (E3) runs here: a leading `SELECT`/`FROM`/`UPDATE` with no GQL
  keyword yields `NotGql`.

**Parser:**
- Outputs an AST that keeps spans for error reporting.
- Errors are a single enum:
  ```rust
  enum ParseError {
      Syntax { span, expected: Vec<&'static str> },
      Unsupported { span, feature_id: &'static str, name: &'static str },
      NotGql { span },
  }
  ```
- `feature_id` constants are generated from `docs/gql-conformance.toml` by `build.rs`,
  so the matrix and the errors cannot disagree.

**Binder:**
- Resolves label, edge-type and property names to their interned ids through the
  catalog (`raft-ranges.md` §9).
- The graph is schemaless (DEC-0007), so an unknown name is not an error. It binds to
  "no such id", which matches nothing on read and allocates an id on write.
- Type-checks expressions against the value types (REQ-0011/0013).

**Planner:**
- Recognises the fast paths in TASK-0018: id lookup becomes a single-key get, and a
  one-hop pattern becomes a single prefix scan.
- Plans DELETE and DETACH DELETE per DEC-0006.

## 5. Open questions for the reviewer

1. **Q1. Source of truth for the feature list** behind `gql-conformance.toml`.
   - The standard is paywalled.
   - Options: the purchased ISO text, the opengql grammar or other public material, or
     a list someone already holds (the E1 checker expects `spec/iso39075-features.txt`).
2. **Q2. A fourth status value.** Does the matrix allow `n/a`, for example for
   graph-type features under DEC-0007? REQ-0003 AC1 names only
   supported/partial/unsupported. Does STORY-0010 E1's `check-matrix.py --require-status`
   accept `n/a`?
3. **Q3. Record the parser choice as a DEC** (hand-written recursive descent plus Pratt)?
4. **Q4. Use the opengql ANTLR grammar as a CI oracle?** It depends on the grammar's
   licence permitting this.
