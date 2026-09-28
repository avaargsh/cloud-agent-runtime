# Security Policy

## Supported versions

These projects are currently pre-1.0 reference implementations. Security fixes are applied to the latest `main` branch.

## Reporting a vulnerability

Please do **not** open a public issue for a suspected vulnerability, leaked credential, authorization bypass, sandbox escape, unsafe remediation path, or other security-sensitive finding.

Use GitHub's private vulnerability reporting feature when it is enabled for the repository. If private reporting is unavailable, contact the maintainer privately through the contact method listed on the GitHub profile.

Include the affected component, reproduction steps, impact, and any suggested mitigation. Avoid including real credentials or sensitive production data.

## Scope

Security-sensitive areas include authorization and approval boundaries, action execution, Temporal signals and workflow identity, evidence integrity, model/tool boundaries, Kubernetes access, sandbox isolation, and dependency supply chain.

This project is experimental and should not be treated as a production security boundary without independent review.