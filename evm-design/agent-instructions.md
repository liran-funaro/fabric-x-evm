# Agent instructions — evm-design

Standing instructions for any agent (LLM) working on the EVM-on-Fabric-X design in this folder. Read
this before editing anything here.

## Document roles and the notes rule

- **`oev-executor-design.md` is the design document, and it must be fully self-contained.** It must
  **not** reference any other document in this folder in any way — no links, no "see `my-idea.md`",
  no "as the source notes say", no mention that a section came from elsewhere. Only internal `§`
  cross-references *within the design doc itself* are allowed. A reader must get full context from
  this one document alone. If a source document stated a requirement that the design addresses,
  formalize that requirement up front in the design doc (e.g. the `R1`–`Rn` list) and refer to it by
  its label later — never by pointing at the source document.

- **`author-notes.md` holds notes from the agent to the author** (Liran). Its purpose is to inform
  the author of inconsistencies between the design and the author's own material — `my-idea.md`,
 `my-notes.md`, `previous-context.md` — and of anything the author should
  decide, verify, or simply be aware of. It is the **only** file that may cross-reference those
  source documents.

- **Consequence:** when the design departs from, resolves, or depends on something in a source
  document, do **not** discuss it in the design doc. Write a note in `author-notes.md` instead. If it
  helps, you may mention the original intent there as a rejected alternative, or explain why an
  alternative approach is unnecessary or fails. Tagging notes by intent — `[decide]` (needs the
  author's call), `[verify]` (a claim in the author's domain to check), `[fyi]` (a resolved
  divergence) — keeps the file scannable.

- Order: The document should be ordered to not refer to future text (when possible). Each part should only refer backwards.
  The requirements should not reveal the design and ideas before hand, but only reflect what is expected
  form the system in terms of behaviour.

- Wording: Use short to the point sentances. Avoid filler words. Use as little words as possible to convey
  a message in a clear manner.

## Audience of the design doc

- The design doc's audience is a **Fabric-X expert with minimal-to-no knowledge of Ethereum or the
  EVM.** Assume the reader knows Fabric-X internals deeply (namespaces, MVCC, the coordinator's
  dependency graph, the orderer/committer pipeline, endorsement policies) and knows essentially
  nothing about Ethereum.

- Therefore, **explain every EVM/Ethereum concept inline the first time it is introduced** —
  accounts, addresses, nonce, storage slots, contract code, gas, state root, receipts, and so on.
  Do not assume any Ethereum terminology is already understood. Keep Fabric-X explanations minimal;
  spend the words on the EVM side.

- Do not explain how Fabric-X works. If we need to refer to exisiting mechanism, mention it without explanining
  how it works.
