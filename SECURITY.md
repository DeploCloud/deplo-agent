# Security Policy

**Please do not report a security vulnerability in a public issue.**

The server agent is the only thing that runs Docker on a host, so a hole here is a hole in
every deplo install. Reports are handled together with the control plane:

- **[Open a private security advisory](https://github.com/DeploCloud/deplo/security/advisories/new)** (preferred), or
- email `security@deplo.build`.

## What happens after you report

| Step                        | When                                                    |
| --------------------------- | ------------------------------------------------------- |
| **Acknowledgement**         | within 72 hours                                         |
| **Assessment and severity** | within 7 days                                           |
| **Coordinated disclosure**  | within 90 days of the report, or as soon as a fix ships |

Every confirmed vulnerability gets a GitHub Security Advisory that credits the reporter, and the
fix ships as a new agent release. Please keep the details private until the advisory is published.

Full policy and scope: **[deplo/SECURITY.md](https://github.com/DeploCloud/deplo/blob/main/SECURITY.md)**.
