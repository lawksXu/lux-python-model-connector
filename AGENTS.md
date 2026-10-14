# Project guide

- This service will own model provider integrations for Lux. The current scope is a minimal FastAPI framework.
- Use Python 3.14 and the `lux_model_connector` package under `src/`.
- Keep dependencies in `pyproject.toml` and install them in a project-local `.venv`. Runtime dependencies are FastAPI and Uvicorn; the `dev` extra provides pytest, HTTPX, and Ruff.
- Add feature packages when needed.
- Add provider SDKs, model invocation APIs, persistence, authentication, and infrastructure only when their requirements are defined.
- Run `python -m pytest`, `python -m ruff check .`, and `python -m ruff format --check .` with the virtual environment activated to validate changes.
- Use HTTPX's ASGI transport for async endpoint tests. It does not run application lifespan; add explicit lifecycle coverage when startup hooks are introduced.
- Start locally with `python -m uvicorn lux_model_connector.main:app --reload --port 8001`. Keep the README current when commands or configuration change.
- `/health` is a lightweight process liveness endpoint. Do not imply that it checks external dependencies or model providers.
- Keep credentials and environment-specific secrets out of Git.
- Keep this root `AGENTS.md` available to Git; nested `AGENTS.md` files are ignored.
- Do not create Git branches or commits unless the user asks.

## Code consistency

- Follow repository formatter and linter configuration. Where no rule exists, follow the nearest maintained code with the same responsibility.
- Use consistent naming, error handling, and dependency usage within each module. Do not copy patterns that conflict with explicit guidance.
- Keep formatting changes limited to the code affected by the task.
- Prefer clear domain names and straightforward control flow. Add comments to explain intent, constraints, or tradeoffs, not obvious operations.

## Explicit data structures

- This rule applies to all Lux repositories and all implementation languages. Whenever a data structure can be defined, it must be represented by an explicit named type with defined fields. Do not substitute `JsonNode`, a generic `Map`/dictionary, `Object`/`Any`, or another broad container for a known business structure.
- Adapt the representation to the language and framework: use Java records/classes and typed variants, or Python Pydantic models, dataclasses, and appropriately defined typed structures. Use explicit type declarations; do not use Java `var` or inferred declarations to obscure an established contract.
- External input with a known schema should be decoded directly into a defined input type. Raw JSON may be used temporarily at an ingress boundary for dynamic input, version detection, protocol adaptation, and validation, but must be converted into defined types before business processing or exchange between business components.
- Genuinely dynamic extension fields and opaque payloads that the component only archives or forwards may remain raw JSON inside an explicitly typed enclosing structure. Document why their schema is not owned by the component. Known fields interpreted by business logic must be modeled explicitly; do not spread string-key lookups across components.
- A parameterized map is appropriate when the domain is genuinely a key-to-value association, or when an external framework requires a map at its adapter boundary. It must not be used as an anonymous record with a fixed set of business fields.
- Explicit types do not replace validation. Preserve required-field, format, ownership, state-transition, and other domain checks. Preserve external JSON field names, optional-field semantics, and persisted formats during internal type refactoring unless a protocol change is explicitly authorized.

## Reuse and abstraction

- Before adding a helper or component, search for existing implementations and inspect their callers.
- Reuse code when its semantics, ownership, and expected changes align. Similar syntax alone is not a reason to share an abstraction.
- Extract shared behavior when it removes meaningful duplication and makes callers easier to understand. Do not create configurable frameworks for hypothetical future requirements.
- Keep domain-specific behavior in its owning module. Put Python utilities in lux-python-utils only when they are domain-independent and have demonstrated reuse.
- Keep provider-specific integration logic in lux-python-model-connector. Do not introduce cross-service calls solely to reuse a small function.
- Single-use helpers are acceptable when they clarify intent or isolate complexity. Avoid helpers and wrappers that only rename an operation.

## Simplicity and scope

- Implement only the behavior required by the current task.
- Avoid empty layers, redundant forwarding methods, unnecessary interfaces, and configuration with no current consumer.
- Validate at trust boundaries and enforce domain invariants. Avoid repeating checks already guaranteed by an internal contract.
- When replacing behavior, remove code made obsolete by the change after checking references, configuration, and dynamic usage.
- Keep cleanup directly related to the task. Do not expand a small change into a repository-wide refactor.

## Test value

- Choose tests based on observable behavior and failure risk, not the number of methods, files, or coverage percentage.
- Prioritize business rules, meaningful boundary cases, failure handling, public contracts, and regression scenarios.
- Do not add tests solely for trivial accessors, framework-generated behavior, constant declarations, or forwarding with no decisions.
- Prefer assertions on results and externally meaningful effects. Verify interactions when ordering, duplication, or invocation is itself part of the contract.
- Mock external boundaries where useful. Avoid mocking the behavior being tested or asserting incidental internal call sequences.
- Use the lowest-cost test level that can expose the failure. Add integration coverage when correctness depends on wiring, serialization, persistence, or another component boundary.
- Consolidate overlapping tests when they protect the same behavior. Before deleting a test, identify any unique scenario it protects.
- Do not introduce public APIs or production branches solely to make tests easier. Improve a boundary only when it also improves production design.
- Documentation-only and formatting-only changes do not require new unit tests. Run the relevant existing checks.

## Completion review

- Check whether the change follows local conventions, reuses suitable existing code, and leaves unnecessary abstractions or obsolete code.
- Explain what each new test protects. If no test is added, explain why when the change affects behavior.
- Report the checks actually run and any remaining verification limits.
