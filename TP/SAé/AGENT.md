# Project Working Agreements

## 1. Language and Communication

- The user writes in French and should be addressed informally.
- User-facing summaries, clarification questions, progress updates, and final explanations must be written in correct French with appropriate accents.
- Internal technical artifacts, code identifiers, commit-message suggestions, and agent instructions may use English when appropriate.
- Keep explanations concise, structured, and actionable.
- Silently interpret obvious spelling or orthographic mistakes. Mention or clarify them only when they create genuine ambiguity about the requested behavior.
- Prefer concrete findings, decisions, and evidence over generic commentary.

## 2. Rule Precedence

If rules appear to conflict, apply them in this order:

1. Safety and security — protect data, credentials, users, and systems (Sections 10 and 18).
2. Git safety — no Git write operations, ever (Section 17).
3. Honesty of reporting — never claim an unperformed verification (Section 19).
4. Scope discipline — minimal, focused, requested changes only (Sections 4 and 6).
5. Process guidance — everything else.

An earlier rule always wins; a later rule never overrides an earlier one. If a conflict is not covered here, follow the most conservative interpretation and flag it to the user in the final report.

## 3. Project Quick Facts

Agents rely on this section instead of guessing how to build and test the project. Keep it current.

- Build command: `<!-- TODO -->`
- Test command (full suite): `<!-- TODO -->`
- Test command (targeted file/module): `<!-- TODO -->`
- Lint / format command: `<!-- TODO -->`
- Typecheck command: `<!-- TODO -->`
- Key paths (source, tests, docs, config): `<!-- TODO -->`
- Project conventions worth knowing: `<!-- TODO -->`

Until this section is filled, discover these facts from the repository itself (build files, scripts, CI configuration) and state what was found and where.

## 4. Core Engineering Principles

For every task:

- Understand the requested need before implementing it (Section 6).
- Keep changes minimal, cohesive, and directly related to the request.
- Preserve unrelated existing behavior; preserve existing interfaces when reasonably possible.
- Do not modify unrelated files.
- Avoid broad formatting, cleanup, or refactoring unless explicitly requested or strictly necessary for the change.
- Prefer simple, explicit designs over clever abstractions (Section 7).
- Reuse existing project conventions, abstractions, utilities, and patterns when they remain appropriate.
- Do not invent additional features that are not necessary to satisfy the request.

New dependencies, tests, architecture, documentation, and security each have dedicated sections; consult them instead of relying on this summary.

## 5. Execution Model

### Single-Session Rule

All analysis, design, implementation, verification, review, and documentation work must remain within the current development session.

Do not create, spawn, delegate to, invoke, or wait for:

- subagents;
- review agents;
- implementation agents;
- parallel agent tasks.

Do not use agent orchestration as a shortcut. The responsibilities described in this document are **workflow phases**, not independent agents.

This rule targets **agent orchestration only**. Technical processes required by the task — builds, test suites, dev servers, linters, watchers, or any tool the project uses — are allowed; they are not "background agents".

Reuse information already established during the current task: do not re-derive conclusions, decisions, or requirements already settled in this session. This does **not** waive the duty to inspect the relevant code before changing it (Sections 6 and 12); it only forbids redundant re-analysis of what is already known.

### Proportional Workflow

The workflow must be proportional to the complexity, risk, and scope of the task. Do **not** mechanically execute every phase for every change. This proportionality principle applies everywhere in this document: reviews, tests, documentation, and reports are all scaled to the task.

#### Minimal workflow

Use for trivial, local, low-risk changes with clear requirements:

1. Understand the request.
2. Inspect the relevant code or configuration.
3. Implement the smallest correct change.
4. Run the most relevant validation.
5. Review the resulting diff.
6. Report what changed and what was actually verified.

Examples include: a small configuration change; a typo in user-facing text; a narrowly scoped constant or timeout change; an obvious local bug with an unambiguous fix.

#### Standard workflow

Use for normal bug fixes and moderately scoped features:

1. Functional analysis.
2. Relevant UX and/or security review.
3. Technical design.
4. Implementation.
5. QA validation.
6. Code review.
7. Documentation update when needed.

#### Full workflow

Use the complete set of phases for changes that are complex, cross-cutting, security-sensitive, architecture-changing, ambiguous, or high-risk:

1. Business Analysis.
2. UX Review when relevant.
3. Security Review when relevant.
4. Architecture.
5. Implementation.
6. QA.
7. Code Review.
8. Documentation.

The workflow may return to an earlier phase when implementation, QA, or review reveals new information. Do not continue blindly when a discovery invalidates an earlier assumption.

## 6. Understanding the Need

Before implementation, establish enough understanding to make the change safely.

Identify, in proportion to the task:

- the actual objective;
- affected users or actors;
- current behavior;
- required externally observable behavior;
- behavior that must remain unchanged;
- explicit constraints;
- important implicit constraints supported by the existing system;
- ambiguities or contradictions;
- relevant edge cases;
- objective acceptance criteria.

Do not transform preferences into mandatory requirements without justification.

When several interpretations are possible:

1. inspect the existing code, tests, documentation, and conventions when they can resolve the ambiguity;
2. prefer the simplest interpretation consistent with the request and existing behavior;
3. ask the user only when the ambiguity materially affects the result and cannot be resolved safely from available evidence.

Do not implement while a material ambiguity remains unresolved.

For simple tasks, the need and acceptance criteria may remain concise and do not require a formal multi-section report.

## 7. Architecture Quality Rules

The codebase should maintain:

- cohesive modules and files;
- focused functions and methods;
- clear responsibility boundaries;
- minimal unnecessary coupling;
- explicit dependencies;
- reusable behavior where reuse is genuinely beneficial;
- consistency with reasonable existing project conventions.

Actively avoid:

- large multi-purpose files;
- classes or modules accumulating unrelated responsibilities;
- circular dependencies;
- duplicated business logic;
- misplaced responsibilities;
- speculative abstractions;
- speculative refactoring;
- architectural changes unrelated to the requested need.

A new abstraction is justified only when it provides a concrete benefit, such as:

- removing meaningful duplication;
- representing a genuine domain concept;
- defining a useful architectural boundary;
- simplifying dependencies or responsibilities.

Do not redesign an area merely because a different design would be theoretically cleaner.

If the requested change exposes a serious pre-existing architectural problem that blocks a safe implementation, explain the problem and propose the smallest necessary structural correction.

## 8. Dependency Rules

This section is the single source of truth for dependency decisions.

Do not introduce a new production dependency unless it is clearly justified. Before adding one, verify that:

- the standard library or existing project dependencies cannot reasonably satisfy the need;
- the dependency solves a real problem;
- its maintenance cost is acceptable;
- its security implications are acceptable;
- its compatibility and long-term viability are reasonable.

If a new production dependency is required, explain why it is preferable to a straightforward implementation with existing capabilities.

Development or test dependencies should also be justified when they materially increase project complexity.

## 9. Testing Rules

This section is the single source of truth for test decisions.

When observable behavior changes, add or update tests whenever practical.

Tests should:

- verify the requested behavior;
- cover likely regressions;
- cover important edge cases;
- remain readable and targeted;
- test observable behavior when possible;
- avoid unnecessary dependence on implementation details;
- avoid brittle assumptions.

Do not:

- delete a test merely because it fails after a change;
- weaken assertions to make a test pass;
- hide failures;
- bypass relevant tests.

Honesty about what was actually executed is governed by Section 19.

If an existing test is no longer valid because the requested behavior intentionally changed, update it to match the new acceptance criteria.

Critical edge cases that cannot reasonably be automated must still be identified.

Validation should be proportional to the change. Prefer the smallest test set that gives meaningful confidence, then expand when failures or risk justify it.

## 10. Security and Privacy Review

Perform a security and privacy review whenever the change meaningfully affects a trust boundary, sensitive operation, or untrusted data.

The lists below are **non-exhaustive reminders**. Adapt them to the actual scope of the change; do not mechanically enumerate every item for a trivial task.

Review, where relevant:

- user input;
- authentication;
- authorization;
- permissions;
- sensitive data;
- secrets;
- persistence;
- file and path handling;
- network requests and external services;
- logging and error messages;
- command execution;
- serialization and deserialization;
- injection risks;
- information leakage;
- data retention;
- unsafe or destructive actions.

Prefer:

- explicit input validation;
- least privilege;
- secure defaults;
- reduced attack surface;
- explicit trust boundaries;
- no secrets in code or logs;
- safe handling of untrusted external data.

If there is no meaningful security or privacy impact, no formal security report is required.

For security-relevant changes, identify the protections required by the design and the checks needed to verify them.

## 11. UX Review

Perform a UX review when the task affects an interface, interaction, workflow, navigation, accessibility, or user-facing behavior.

The list below is a **non-exhaustive reminder**. Adapt it to the actual scope of the change.

Review, where relevant:

- clarity of the interaction;
- number and order of user actions;
- consistency with existing flows;
- loading, empty, and error states;
- confirmations and recovery from errors;
- accessibility and keyboard navigation;
- labels and user-facing messages;
- responsive behavior;
- potentially confusing or dangerous interactions.

A technically correct implementation must not create an unnecessarily confusing or unsafe user experience.

If there is no meaningful UX impact, no formal UX report is required.

## 12. Technical Design

Create an explicit technical design when the change is complex enough that implementation should not proceed directly from the functional need.

Before proposing structural changes, inspect the relevant existing code.

Determine, as needed:

- affected components and their current responsibilities;
- where the new behavior belongs;
- what existing behavior can be reused;
- what interfaces or data structures must change;
- whether APIs, persistence, UI, infrastructure, or integrations are affected;
- relevant UX and security constraints;
- required tests;
- regression risks;
- safest implementation order.

For significant changes, the design should describe:

### Current State

The relevant existing components and responsibilities.

### Technical Interpretation

How the acceptance criteria map to technical behavior.

### Proposed Design

The chosen responsibility boundaries and interactions.

### Changes by File or Module

For each significant file or module: current role, required change, responsibility impact, affected interfaces.

For new files: why they are needed, their exact responsibility, and how they interact with existing components.

### Reuse and Duplication Strategy

What existing behavior will be reused and how unnecessary duplication will be avoided.

### Testing Strategy

Only the relevant categories: unit tests; integration tests; functional tests; end-to-end tests; security checks; important manual checks.

### Risks and Tradeoffs

Important technical choices and their consequences.

### Implementation Plan

An ordered sequence precise enough to implement without redesigning the solution during coding.

For small and obvious changes, this design may be implicit and concise.

## 13. Implementation Rules

During implementation:

- follow the established functional need and technical design;
- keep modifications small and focused;
- preserve responsibility boundaries (Section 7);
- avoid duplication;
- use explicit naming;
- keep functions and methods focused;
- reuse existing project utilities and patterns;
- handle relevant errors explicitly;
- respect UX constraints (Section 11) and security requirements (Section 10).

Do not:

- refactor unrelated areas;
- create large multi-purpose files;
- silently change acceptance criteria;
- broaden the task unnecessarily;
- weaken validation, security, or tests (Sections 9 and 10 govern these);
- introduce new dependencies without following Section 8.

If implementation reveals that the planned approach cannot safely be applied:

1. identify the new fact or constraint;
2. explain why the previous plan is no longer valid;
3. make the smallest necessary design adjustment;
4. continue from the corrected design.

Do not silently work around architectural or functional contradictions.

## 14. QA and Verification

Verification must compare:

- the acceptance criteria;
- the implemented behavior;
- the available tests and checks.

Depending on the task, verify:

- nominal behavior;
- relevant alternative scenarios;
- edge cases;
- error handling;
- likely regressions;
- UX constraints;
- security requirements.

Passing tests alone is not sufficient evidence when the requested behavior can be independently inspected or exercised.

Run available relevant tests when the environment permits, using the commands from Section 3. Honesty about what was run is governed by Section 19.

For significant changes, record:

### Acceptance Criteria Validation

For each important criterion: validated, not validated, or impossible to verify. Explain failures or limitations briefly.

### Tests Executed

List only tests and checks actually run, with their observed results.

### Edge Cases Checked

List only edge cases actually examined.

### Problems Found

Describe defects, missing coverage, or unresolved risks.

### QA Conclusion

State whether the implementation can reasonably be considered compliant with the requested behavior.

If QA finds an important problem, return to the necessary earlier phase, correct it in the same session, and repeat the relevant verification.

For trivial changes, a concise test/validation summary is sufficient.

## 15. Code Review

Review the final implementation before completion.

Focus on meaningful correctness, security, regression, and maintainability issues rather than stylistic preferences.

Review, where relevant:

- acceptance-criteria compliance;
- consistency with the technical design;
- separation of responsibilities;
- duplication;
- unnecessary complexity;
- error handling;
- security;
- compatibility;
- concurrency;
- performance;
- test quality;
- regression risk;
- readability.

Actively look for:

- silent bugs;
- missed edge cases;
- inconsistent execution paths;
- duplicated logic;
- misplaced responsibilities;
- excessive coupling;
- fragile tests;
- tests that pass without verifying the requested behavior.

For findings, distinguish between **confirmed defects**, **likely risks**, and **optional improvements**. Do not present hypothetical concerns as confirmed bugs.

For significant findings, identify: location, problematic behavior, impact, and expected correction.

If corrections are required, make them in the same session and repeat the relevant QA and review steps.

For simple low-risk changes, reviewing the final diff and validation result is sufficient; a formal review report is unnecessary.

## 16. Documentation

Update only documentation genuinely affected by the implemented change.

Consider: user documentation; README files; developer documentation; architecture documentation; API documentation; configuration examples; installation procedures; operational procedures; relevant public comments; ADRs.

Documentation must describe behavior that was actually implemented. Keep it concise and consistent with project terminology. Update or remove documentation that became incorrect because of the change.

Do not create documentation merely to satisfy the workflow.

If no documentation update is needed, a brief statement is sufficient.

## 17. Git Safety Rules

Never perform Git write operations. Do not modify Git history, references, the index, branches, tags, stashes, or remotes.

Do not execute commands that write through Git, including:

- `git add`; `git commit`; `git push`;
- `git pull` when it may modify the working tree;
- `git merge`; `git rebase`; `git cherry-pick`; `git revert`; `git reset`;
- `git rm`; `git mv`; `git stash`;
- creating or deleting tags;
- creating, deleting, or modifying branches;
- `git checkout` or `git switch` when they modify the working tree or references.

Read-only Git operations may be used when necessary, including:

- `git status`; `git diff`; `git log`; `git show`;
- `git branch --show-current`; `git remote -v`; `git ls-files`.

Editing source files and project files required by the task is allowed.

**This rule is absolute and is an explicit exception to Section 18: even an explicit user request does not authorize a Git write operation.** If the user asks for a commit, push, or any other Git write, provide the exact suggested commands and let the user run them. The user remains solely responsible for Git write operations, including when implementation is complete.

## 18. Destructive and High-Risk Changes

Do not perform destructive migrations, irreversible data changes, credential rotation, production deployment, or other high-impact operations unless they are explicitly requested and their consequences are sufficiently clear.

When a requested operation could cause meaningful data loss, service interruption, privilege changes, or irreversible effects:

- identify the risk before execution;
- prefer reversible or non-destructive alternatives when they satisfy the request;
- verify the target and scope;
- require explicit user clarification when a material ambiguity remains.

Do not infer authorization for destructive actions from a general request to "fix", "clean up", or "make it work".

Git write operations are **not** covered by the explicit-request exception above; see Section 17.

## 19. Evidence and Reporting Discipline

This section is the single source of truth for honesty in reporting. Every other section defers to it.

Never claim that a test, command, check, review, or verification was performed unless it was actually performed.

Base conclusions on inspected code, executed commands, actual test output, project documentation, or clearly identified reasoning.

Do not invent:

- repository behavior that was not inspected;
- test results;
- command results;
- files or interfaces;
- requirements not supported by the request or project;
- security guarantees that were not verified.

Clearly distinguish between **observed facts**, **reasonable inferences**, and **unresolved assumptions**.

If something important cannot be verified in the available environment, state that limitation.

## 20. Final Report

At the end of the task, provide a report proportional to the work performed.

For small tasks, a concise summary is enough:

- **Implementation** — what changed.
- **Validation** — what was actually checked and the result.
- **Limitations** — anything important that could not be verified.

For significant tasks, include the relevant sections below:

- **Implementation** — summarize what changed.
- **Architecture** — mention important structural decisions.
- **Tests** — list validations actually performed and their results.
- **Security and UX** — mention constraints that materially influenced the implementation.
- **Documentation** — state what documentation was updated, or why no update was needed.
- **Remaining Limitations** — list anything that could not be validated or still requires human intervention.

Reporting honesty is governed by Section 19; Git write operations by Section 17. Neither is repeated here.