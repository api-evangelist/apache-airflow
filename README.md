# Apache Airflow (apache-airflow)

<!-- API-EVANGELIST-PROVENANCE:BEGIN -->
> ### About this repository
>
> **This is not our API.** This repository is an independent, third-party profile of a company's
> **publicly available** API surface, maintained by [API Evangelist](https://apievangelist.com).
> API Evangelist does not operate, host, resell, or support this company's APIs, and is not
> affiliated with or endorsed by the company unless stated on the profile.
>
> **Where the information came from.** Everything here is assembled from material a member of the
> public can reach with a browser and no credentials — the company's own website, developer portal
> and documentation, the specifications it publishes for public use (OpenAPI, AsyncAPI, JSON Schema,
> `apis.json`, `llms.txt` and similar), its public repositories, and its public status, pricing and
> changelog pages. **Nothing here is obtained by breaching a system, defeating an access control, or
> using credentials of any kind.**
>
> **The rating is an independent assessment.** The Kin Score and Agent Readiness rating are
> independently calculated scores of a company's *public* API artifacts, produced by API Evangelist
> against a published rubric. They are not certifications, endorsements, security assessments, or
> audits, and they score published artifacts — not the quality, safety, or security of the software.
>
> **Corrections, re-scores, and removal are free.** No partnership, contract, or purchase is
> required, and you do not need to justify the request.
>
> - **Something wrong?** Open an issue on this repository, or email
>   [info@apievangelist.com](mailto:info@apievangelist.com).
> - **Published something new?** Ask for a re-score and we will re-run the rating.
> - **Want the listing taken down?** Say so and we will honor it. The profile is reduced to your
>   company name, a factual description, and a link to your own site, and the company is recorded as
>   **unrated** — never scored zero for having asked.
>
> **Response times.** Acknowledgement within **one business day**; removal or restriction within
> **two business days**; corrections and re-scores within **five business days**.
>
> **On a security or compliance team?** Email
> [info@apievangelist.com](mailto:info@apievangelist.com) with *security* in the subject line and
> you will get a person, not a form. We will tell you exactly which public URLs this profile was
> built from so your team can see the same surface we did, and we will take the listing down on
> request while you work through it.
>
> Full detail: **[Where this data comes from](https://apievangelist.com/about/where-our-data-comes-from)**
<!-- API-EVANGELIST-PROVENANCE:END -->

Apache Airflow is an open-source platform to programmatically author, schedule, and monitor workflows, developed by the Apache Software Foundation. It allows you to define workflows as Directed Acyclic Graphs (DAGs) in Python code, making them maintainable, versionable, testable, and collaborative. Airflow provides a stable REST API for managing DAGs, DAG runs, tasks, connections, variables, pools, and users, along with a web-based UI for monitoring and managing pipeline execution.

**URL:** [https://airflow.apache.org/](https://airflow.apache.org/)

**Run:** [Capabilities Using Naftiko](https://github.com/naftiko/fleet?utm_source=api-evangelist&utm_medium=readme&utm_campaign=company-api-evangelist&utm_content=repo)

## Tags

 - Apache, DAG, Data Pipeline, ETL, Open Source, Orchestration, Python, Scheduling, Workflow

## Timestamps

- **Created:** 2024-01-15
- **Modified:** 2026-04-19

## APIs

### Apache Airflow REST API
The stable public REST API for interacting with Apache Airflow programmatically, allowing management of DAGs, DAG runs, task instances, connections, variables, pools, roles, users, and monitoring resources.

**Human URL:** [https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)

#### Tags

 - DAGs, REST, Tasks, Workflow

#### Properties

- [Documentation](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)
- [OpenAPI](openapi/apache-airflow-openapi.yaml)
- [Authentication](https://airflow.apache.org/docs/apache-airflow/stable/security/api.html)
- [ChangeLog](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html)

### Apache Airflow Experimental API (Deprecated)
The experimental API that preceded the stable REST API. This is deprecated and should not be used for new implementations.

**Human URL:** [https://airflow.apache.org/docs/apache-airflow/stable/deprecated-rest-api-ref.html](https://airflow.apache.org/docs/apache-airflow/stable/deprecated-rest-api-ref.html)

#### Tags

 - Deprecated, Legacy, REST

#### Properties

- [Documentation](https://airflow.apache.org/docs/apache-airflow/stable/deprecated-rest-api-ref.html)

## Common Properties

- [GitHubOrganization](https://github.com/apache)
- [GitHubRepository](https://github.com/apache/airflow)
- [Documentation](https://airflow.apache.org/)
- [GettingStarted](https://airflow.apache.org/docs/apache-airflow/stable/start.html)
- [Tutorials](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/index.html)
- [Python Package (PyPI)](https://pypi.org/project/apache-airflow/)
- [Docker Image](https://hub.docker.com/r/apache/airflow)
- [Security](https://airflow.apache.org/docs/apache-airflow/stable/security/)
- [Blog](https://airflow.apache.org/blog/)
- [Support](https://airflow.apache.org/community/)
- [ChangeLog](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html)

## Features

| Name | Description |
|------|-------------|
| DAG-as-Code | Define workflows as Python code (Directed Acyclic Graphs) for version control, testing, and collaboration. |
| Stable REST API | Full-featured REST API for programmatic management of DAGs, runs, tasks, connections, variables, pools, and users. |
| Dynamic Pipeline Generation | Generate DAGs dynamically using Python, supporting complex conditional and parametric pipelines. |
| Extensible Providers | Rich ecosystem of provider packages for integrating with AWS, GCP, Azure, databases, and hundreds of external services. |
| Rich Web UI | Browser-based dashboard for monitoring DAG runs, task statuses, logs, and Gantt charts. |
| Resource Pools | Control concurrency and resource allocation across tasks using configurable pools. |
| Cross-DAG Dependencies | Define dependencies between DAGs using sensors, dataset-driven scheduling, and external task sensors. |
| Pluggable Executors | Supports Sequential, Local, Celery, Kubernetes, and DASK executors for flexible deployment. |
| SLA Monitoring | Define and track Service Level Agreements on task and DAG completion times. |
| Variable and Connection Management | Centrally manage environment-specific configuration via Airflow variables and connections. |

## Use Cases

| Name | Description |
|------|-------------|
| ETL Pipeline Orchestration | Schedule and manage extract, transform, load pipelines with dependency management and retry logic. |
| Machine Learning Workflows | Orchestrate ML training, validation, and deployment pipelines with data dependency tracking. |
| Data Warehouse Loading | Coordinate data ingestion from multiple sources into data warehouses like BigQuery, Redshift, and Snowflake. |
| Batch Report Generation | Schedule periodic batch reporting jobs with email notification on completion or failure. |
| Multi-Cloud Data Movement | Move data between AWS, GCP, and Azure using provider integrations with dependency control. |
| CI/CD Pipeline Orchestration | Trigger and monitor software deployment pipelines with upstream/downstream task dependencies. |

## Integrations

| Name | Description |
|------|-------------|
| Apache Spark | Native Spark submit and Livy operator integration for distributed data processing. |
| Google Cloud | Comprehensive GCP provider for BigQuery, Cloud Storage, Dataflow, Dataproc, and more. |
| Amazon Web Services | AWS provider for S3, Redshift, EMR, Glue, Lambda, and other services. |
| Microsoft Azure | Azure provider for Blob Storage, Data Factory, HDInsight, and Databricks. |
| dbt | dbt operator for running dbt transformations within Airflow pipelines. |
| Kubernetes | KubernetesPodOperator for running tasks in isolated Kubernetes pods. |
| Docker | DockerOperator for running tasks in Docker containers with isolated environments. |
| Apache Kafka | Kafka producers and consumers as Airflow tasks via the Kafka provider. |

## Artifacts

Machine-readable API specifications organized by format.

### OpenAPI

- [Apache Airflow REST API](openapi/apache-airflow-openapi.yaml)

### JSON Schema

84 schema files extracted from the Airflow REST API OpenAPI specification covering DAGs, DAG Runs, Task Instances, Connections, Variables, Pools, Users, Roles, and more.

### JSON-LD

- [Apache Airflow Context](json-ld/apache-airflow-context.jsonld)

## Capabilities

Naftiko capabilities organized as shared per-API definitions composed into customer-facing workflows.

### Shared Per-API Definitions

- [Apache Airflow REST API](capabilities/shared/airflow-rest.yaml) — 11 operations for DAG, task, connection, variable, and pool management

### Workflow Capabilities

| Workflow | APIs Combined | Tools | Persona |
|----------|--------------|-------|---------|
| [Apache Airflow Workflow Orchestration](capabilities/airflow-orchestration.yaml) | Airflow REST | 7 | Data Engineer, Platform Operator |

## Vocabulary

- [Apache Airflow Vocabulary](vocabulary/apache-airflow-vocabulary.yaml) — Unified taxonomy mapping 12 resources, 8 actions, 1 workflow, and 2 personas across operational (OpenAPI) and capability (Naftiko) dimensions

## Rules

- [Apache Airflow Spectral Rules](rules/apache-airflow-spectral-rules.yml) — 19 rules across 8 categories enforcing Apache Airflow API conventions

## Maintainers

**FN:** Kin Lane

**Email:** info@apievangelist.com
