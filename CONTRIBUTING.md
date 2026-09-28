# Contributing

Thanks for contributing. These repositories are engineering reference implementations, so changes should preserve explicit control-plane boundaries and reproducibility.

## Workflow

1. Open an issue for substantial behavior or architecture changes.
2. Create a focused branch and keep changes small enough to review.
3. Add or update tests for behavior changes.
4. Run the repository test suite locally.
5. Open a pull request describing the problem, design choice, validation, and any safety/correctness implications.

## Engineering expectations

- Prefer explicit contracts over hidden framework behavior.
- Keep model decisions separate from deterministic authorization.
- Preserve replayability, evidence lineage, and idempotency where applicable.
- Do not commit credentials, private endpoints, customer data, or proprietary datasets.
- New dependencies should have a clear engineering justification.

## Pull requests

A PR should explain **what changed**, **why**, **how it was tested**, and **what remains intentionally out of scope**. Experimental work should be labeled as such and must not be presented as production-ready.

By contributing, you agree that your contributions are licensed under the repository's Apache License 2.0.