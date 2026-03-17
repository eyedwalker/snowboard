# Encompass Assist — Agent & Platform Architecture

## Overview

Encompass Assist is a fully serverless, multi-tenant AI platform built on AWS. It provides conversational AI assistants for VSP Vision Care eye care professionals, combining knowledge base retrieval (RAG) with agentic analytics capabilities powered by a Snowflake data warehouse.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ENCOMPASS ASSIST PLATFORM                          │
└─────────────────────────────────────────────────────────────────────────────┘

 USER INTERFACES
 ───────────────
 ┌──────────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
 │  Encompass Assist    │     │  Chat Configurator   │     │  Streamlit UI   │
 │  (Widget — JS)       │     │  (Angular Admin)     │     │  (Local Dev)    │
 │                      │     │                      │     │                 │
 │  Embedded in         │     │  yabba-dabba-do.com  │     │  Claude API     │
 │  Encompass PM        │     │  S3 + CloudFront     │     │  tool_use loop  │
 │                      │     │                      │     │                 │
 │  Async polling       │     │  Assistants, KBs,    │     └─────────────────┘
 │  Inline SVG charts   │     │  Report Schedules,   │
 │  Markdown rendering  │     │  Test Suites, Team   │
 └──────────┬───────────┘     └──────────┬───────────┘
            │                            │
 API LAYER  │                            │
 ──────────────────────────────────────────
            ▼                            ▼
 ┌──────────────────────────────────────────────────────────────────┐
 │              HTTP API Gateway                                    │
 │              Cognito JWT auth (admin) + API-key auth (widget)   │
 │              1,000 req/s rate, 500 burst                        │
 │              30s timeout → async polling for long operations     │
 └──────────┬────────────────────────────────┬─────────────────────┘
            │                                │
      Widget Routes                    Admin Routes
      (API-key, no JWT)               (JWT + RBAC)
            │                                │
            ▼                                ▼
 ┌─────────────────────┐        ┌─────────────────────────┐
 │  API Lambda          │        │  API Lambda (same)       │
 │  chat-agent-api-dev  │        │  /assistants, /team,     │
 │  Node.js 20 | 120s   │       │  /report-schedules,      │
 │                      │        │  /knowledge-bases, etc.  │
 │  Creates async job   │        └─────────────────────────┘
 │  → DynamoDB record   │
 │  → Fires Lambda      │
 └──────────┬───────────┘
            │
            ▼
 ┌─────────────────────────┐
 │  Provision Lambda        │
 │  chat-agent-provision    │
 │  Node.js 20 | 300s      │
 │                          │
 │  • _chatJob → Bedrock    │
 │  • _crawlJob → Web crawl │
 │  • _reportJob → Report   │
 │    generation + delivery  │
 └──────────┬───────────────┘
            │
 AI / LLM LAYER
 ──────────────
            ▼
 ┌──────────────────────────────────────────────────────────────────┐
 │                    BEDROCK AGENT                                 │
 │                    "Encompass-Larry" (KBAQR27COL)                │
 │                                                                  │
 │  LLM: Claude Sonnet 4.6 (us.anthropic.claude-sonnet-4-6)       │
 │  Extended thinking enabled (1024 budget tokens)                  │
 │  Alias: TSTALIASID (DRAFT)                                      │
 │  Session attributes: { tenantId } for multi-tenant isolation     │
 │                                                                  │
 │  ┌────────────────────────┐    ┌────────────────────────┐       │
 │  │  Action Group:         │    │  Knowledge Base:        │       │
 │  │  snowflake-analytics   │    │  CSJA2NKXQS            │       │
 │  │  (10 API paths)        │    │  Encompass Help Docs   │       │
 │  │                        │    │                        │       │
 │  │  Schema Discovery      │    │  Embedding Model:      │       │
 │  │  Query Execution       │    │  Amazon Titan          │       │
 │  │  Pre-built Analytics   │    │  Embed Text v2         │       │
 │  │  Report Generation     │    │                        │       │
 │  │  Report Scheduling     │    │  Vector Store:         │       │
 │  └────────────────────────┘    │  S3 Vectors            │       │
 │                                └────────────────────────┘       │
 └──────────────────────────────────────────────────────────────────┘
```

---

## LLM Models

| Model | Provider | Model ID | Purpose |
|-------|----------|----------|---------|
| **Claude Sonnet 4.6** | Anthropic (via Bedrock) | `us.anthropic.claude-sonnet-4-6` | Agent orchestration — tool selection, multi-turn reasoning, response generation. Extended thinking enabled (1024 budget tokens). |
| **Amazon Titan Embed Text v2** | Amazon (Bedrock) | `amazon.titan-embed-text-v2:0` | Document embedding for RAG knowledge base retrieval via S3 Vectors. |
| **Mixtral 8x7B** | Snowflake Cortex | `mixtral-8x7b` (Snowflake-hosted) | In-database LLM inference and text summarization via SQL functions. |

---

## Agent Tool Inventory

| Category | Tools | Source | Description |
|----------|-------|--------|-------------|
| Schema Discovery | `list_tables`, `describe_table` | Snowflake Lambda | Browse tables, views, columns in the data warehouse |
| Query Execution | `run_query` | Snowflake Lambda | Execute read-only SQL against Snowflake (SELECT only) |
| Pre-Built Analytics | `revenue_summary`, `patient_summary`, `top_products`, `appointment_utilization` | Snowflake Lambda | Business KPIs with parameterized date ranges and filters |
| Report Generation | `generate_report`, `generate_chart` | Snowflake Lambda | Excel/CSV exports and SVG chart visualizations → S3 + CDN |
| Report Scheduling | `schedule_report` | API Lambda (forwarded) | Create recurring reports delivered via email/SMS |
| Knowledge Base | RAG retrieval | Bedrock KB (CSJA2NKXQS) | Semantic search over Encompass help docs and VSP policies |

---

## Scheduled Report Delivery

```
EventBridge Scheduler ──→ Provision Lambda ──→ Snowflake Lambda ──→ S3
     (cron trigger)        (read schedule,      (execute SQL,       (Excel/CSV/SVG)
                            create run)          generate report)        │
                                │                                        │
                                ├──→ SES Email (secure download link) ◄──┘
                                └──→ Twilio SMS (download link)

 HIPAA: No PHI in email/SMS body — only secure download links.
 Auto-disable after 3 consecutive failures.
 90-day run history retention (DynamoDB TTL).
 7-day report file retention (S3 lifecycle).
```

---

## Data Stores

| Store | Type | Content | Retention |
|-------|------|---------|-----------|
| Snowflake (DEV_ANALYTICS) | Data Warehouse | 118 business views + 74 datamart tables — patients, orders, billing, appointments, insurance, products | Persistent |
| DynamoDB (18+ tables) | NoSQL | Assistants, tenants, metrics, schedules, runs, team, hierarchy, content, test suites | PITR + SSE |
| S3 (snowflake-eyecare-reports-dev) | Object Storage | Generated Excel/CSV reports and SVG charts | 7-day lifecycle |
| S3 (wubba-data-sources) | Object Storage | Knowledge base documents, OpenAPI schemas | Persistent |
| S3 Vectors | Vector Store | Embedded document chunks for RAG retrieval | Persistent |
| Secrets Manager | Secrets | Snowflake credentials | Encrypted, rotatable |
| SSM Parameter Store | Config | Email from-address, Twilio credentials per tenant | Per-tenant scoped |

---

## Security Architecture

| Control | Implementation |
|---------|---------------|
| **Authentication** | Amazon Cognito with SRP protocol, JWT tokens (1hr access, 30-day refresh) |
| **Authorization (RBAC)** | admin, editor, viewer roles enforced at API and UI levels |
| **API Key Isolation** | Widget endpoints use per-assistant API keys scoped to tenant data |
| **Data Encryption** | DynamoDB SSE (AES-256), S3 SSE, HTTPS/TLS everywhere |
| **Multi-Tenant Segregation** | All queries filtered by `tenantId` — no cross-tenant data leakage |
| **AI Guardrails** | Bedrock Guardrails for content filtering, PII detection, topic restrictions |
| **SQL Safety** | Read-only enforcement (SELECT only), forbidden keyword regex |
| **HIPAA Compliance** | No PHI in email/SMS, secure download links with expiry, HIPAA S3 bucket |
| **Audit Trail** | DynamoDB audit log, Point-in-Time Recovery, run history with TTL |
| **Infrastructure as Code** | SAM template.yaml — reproducible, auditable, version-controlled |

---

## Infrastructure

| Service | Purpose | Config |
|---------|---------|--------|
| AWS Lambda (Node.js 20) | API compute — 3 functions: API (120s), Provision (300s), Test Runner (900s) | 512 MB, ARM64, pay-per-request |
| AWS Lambda (Python 3.11) | Snowflake action group — data tools, report generation | 1024 MB, x86_64 |
| Amazon API Gateway (HTTP API) | REST API with JWT + API-key auth, CORS | 1,000 req/s rate, 500 burst |
| Amazon Cognito | User authentication — email/password with SRP | Pool + client, 1hr tokens |
| Amazon Bedrock Agents | Agentic AI orchestration — tool selection, reasoning | Encompass-Larry, extended thinking |
| Bedrock Knowledge Bases | RAG retrieval with S3 Vectors | Titan Embed Text v2 |
| Amazon EventBridge Scheduler | Timezone-aware cron for scheduled reports | Per-schedule, retry + DLQ |
| Amazon DynamoDB | Primary datastore — 18+ tables | On-demand, PITR, SSE |
| Amazon S3 | Frontend hosting, KB content, reports/charts | CloudFront-distributed |
| Amazon CloudFront | CDN — yabba-dabba-do.com + reports.wubba.ai | HTTPS, custom domains, ACM |
| Amazon SES | Email delivery for reports and escalations | Domain: wubba.ai, DKIM verified |
| Snowflake | Eyecare practice data warehouse | eyefinity-dev, DEV_ANALYTICS |
| AWS SAM / CloudFormation | Infrastructure as Code | Parameterized (dev/prod) |
| Angular 17 | Frontend SPA with Material Design | 15+ feature modules, lazy-loaded |

---

## Feature Matrix

| Feature | Status |
|---------|--------|
| AI Chat (Bedrock Agents with RAG) | Live |
| Snowflake Analytics Agent | Live |
| Report Generation (Excel/CSV/SVG Charts) | Live |
| Scheduled Report Delivery (Email/SMS) | Live |
| Knowledge Bases (multi-KB, web crawl, BDA) | Live |
| Screen Mappings (context-aware prompts) | Live |
| Embeddable Widget (JS, screen context) | Live |
| Test Suites (AI quality evaluation) | Live |
| Hierarchy & RBAC | Live |
| Escalation / Support Cases | Live |
| Team Management | Live |
| Analytics Dashboard | Live |
| Widget Presets (theming) | Live |
| API Documentation (Swagger UI) | Live |
| Multi-Tenant Isolation | Live |

---

*Generated March 2026 — Encompass Assist Platform v2*
