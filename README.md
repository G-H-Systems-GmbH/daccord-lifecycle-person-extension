# daccord Lifecycle Extension – Person

A [daccord](https://www.daccord.de) Lifecycle Extension that consumes Kafka events for **PersonCandidate**, **Person** and **Policy** nodes and drives the HR lifecycle of a Person through CIB Seven workflows and direct attribute updates.

## Concept

daccord's Person lifecycle follows a staging pattern:

1. An HR source system (SAP SuccessFactors, MS Dynamics, AD, …) feeds its data into **PersonCandidate** nodes via a Data Collection. A PersonCandidate is a staging object — it has no IAM identity yet and no accounts.
2. A converting script computes `relevantforadd` / `relevantfordelete` on the PersonCandidate (e.g. "within 30 days before entry date" → `true`).
3. A change to one of these properties is picked up by daccord's Neo4j event mechanism and published to the Kafka topic **`lifecycle_personcandidates`**.
4. This extension consumes the event, detects which action it represents, loads the related data (company/location/orgunit are resolved via their `code` property) and either starts a workflow or writes the change directly to the graph.
5. For an entry, the workflow creates/updates the final **Person** node (the actual IAM identity, the one that owns accounts). The PersonCandidate is kept afterwards so further changes can still be detected; `(PersonCandidate)-[:REPRESENTS]->(Person)` links the two.
6. In parallel, daccord's Policy engine periodically evaluates time-based conditions (e.g. "leavedate in the past") and raises **Policy events**, which this extension also consumes to drive status transitions (`active → inactive → readytodelete → deletioninprogress → deleted`).

The extension itself only detects *what* happened and triggers the right reaction — the actual business process (approvals, provisioning steps) lives in BPMN processes, not in this code.

> **Workflow engine naming:** Since daccord release 2.2.2, the workflow engine used in production is **CIB Seven** (a Camunda 7-compatible fork), not Camunda. The Java/plugin API this project depends on (`CamundaVariable`, `CamundaAction`, `CamundaSession`, `DACWorkflowEngine.type_DACCORD_CAMUNDA`, `camundaurl()`) still uses the "Camunda" name for compatibility reasons — that's a naming artifact of the underlying plugin, not a statement about which engine is actually running.

## What it consumes

The consumer (`DaccordPersonLifecycleConsumerTask`) routes incoming events by label into three handlers:

| Event source | Handler | Reacts to |
|---|---|---|
| `PersonCandidate` | `PersonCandidateTaskHandler` | Entry, leaving, org/name/manager/absence changes, other attribute changes, deletion |
| `Person` | `PersonTaskHandler` | The same kind of changes, detected directly on the final Person node |
| `PolicyStatistics` | `PolicyTaskHandler` | Time-based policy checks (see below) |

### PersonCandidate actions

Detected from `CREATE_NODE` / `MODIFY_NODE` / `DELETE_NODE` events and their changed properties:

| Changed property / event | Action | Effect |
|---|---|---|
| `relevantforadd` → `true` (create or modify) | `ENTRY` | Starts workflow `person-internal-add-auto` *(currently commented out, see note below)* |
| `relevantfordelete` → `true`, or node deleted | `LEAVING` | Starts workflow `person-internal-delete-auto` |
| `companycode` / `orgunit1-3code` | `CHANGE_OU` | Updates org data on the Person node |
| `surname` / `givenname` | `CHANGE_NAME` | Updates name on the Person node |
| `manageruniqueid` | `CHANGE_RESP` | Updates the responsible/manager on the Person node |
| `dateofabsencestart` / `dateofabsenceend` / `systemaccess` / `statusreason` | `CHANGE_ABSENCE` | Updates absence data on the Person node |
| any other attribute (phone, mobile, costcenter, position, jobtitle, leavedate, …) | `MODIFY` | Updates the changed attributes on the Person node |
| node deleted (PersonCandidate already removed) | `DELETE` | Cleans up the corresponding Person reference |

Only `ENTRY` and `LEAVING` start a CIB Seven workflow; all other actions write the change directly to the `Person` node via Cypher.

> **Note:** `startWorkflow()` in `DaccordPersonLifecycleConsumerTask` has the actual `workflowEngine.startProcess(...)` call commented out, and the `ENTRY` workflow call in `PersonCandidateTaskHandler.handleEntry()` is likewise commented out. This is intentional — it lets the extension be run and logged without actually starting processes. Uncomment both before relying on this for real ENTRY/LEAVING processing.

### Person actions

`PersonTaskHandler` mirrors the same categories (`STATUS_TO_INACTIVE`, `STATUS_TO_ABSENCE`, `NAME_CHANGE`, `ORG_CHANGE`, `RESPONSIBLE_CHANGE`, `BASISDATEN_CHANGE`, `LEAVEDATE_CHANGE`) for changes detected directly on the Person node. These currently only log the detected action — the downstream provisioning/account-update calls are marked `TODO` and not yet wired up.

### Policies

`PolicyTaskHandler` resolves the triggering policy's name from the graph and routes by name:

| Policy name | Purpose |
|---|---|
| `check leavedate for deactivation` | Active persons whose `leavedate` has passed → `status = inactive` |
| `check leavedate for readytodelete` | Inactive persons, 14 days past `leavedate` → `status = readytodelete` |
| `check leavedate for deletionprocess` | `readytodelete` persons, 28 days past `leavedate` → `status = deletioninprogress` |
| `check leavedate for finaldeletion` | `deletioninprogress` persons with no accounts, 30 days past `leavedate` → deleted |
| `check absencestartdate for absencestart` | Active persons whose `absencestartdate` has been reached → `status = absence` |
| `check absenceenddate for absenceend` | Absent persons whose `absenceenddate` has passed → `status = active` |

These policy names must match exactly what is configured on the corresponding `Policy` nodes in daccord — an unknown name is logged as a warning and skipped.

## Prerequisites

- Java 11
- Maven 3.9+
- daccord with the Lifecycle Extension event stream configured (Integration Package ID **101**, fixed in `Main.java`)
- CIB Seven as the configured workflow engine, with the referenced process keys deployed (`person-internal-add-auto`, `person-internal-delete-auto`)

## Building

```bash
mvn clean package -q
```

The runnable JAR is built to `target/daccord-lifecycle-person-extension-1.0.0-SNAPSHOT-runnable.jar`.

## Configuration

`config.xml` provides the SystemDB connection and a few core daccord settings (see `example/config.xml` for a template):

| Parameter | Description |
|---|---|
| `system/systemdbprotocol` | Bolt protocol for the Neo4j connection |
| `system/systemdbhost`, `systemdbport`, `systemdbname` | SystemDB connection details |
| `system/systemdbuser`, `systemdbpassword` | SystemDB credentials (`systemdbpassword` is encrypted by daccord) |
| `system/systemid` | daccord system ID |
| `base/verificationcode` | Verification code used when creating the DACSession |
| `base/decryptionhash` | Value used by daccord to decrypt encrypted config parameters |

## Running (standalone test)

```bash
java -Dlog4j2.level=DEBUG -jar target/daccord-lifecycle-person-extension-1.0.0-SNAPSHOT-runnable.jar \
  example/config.xml \
  example/test_event_entry.xml
```

> **Note:** Always include `-Dlog4j2.level=DEBUG`, otherwise there is no console output.

- With only `config.xml` given, a default `ENTRY` test event is used.
- With a second argument, the test file's `<eventCollection>` is loaded and dispatched to the consumer; the referenced `sourcenodeuniqueid` must exist as a `PersonCandidate` in the connected database.
- See `example/test_event_*.xml` for sample events covering entry, name/org/manager/absence changes, deletion and each policy.

## Project structure

```
src/main/java/de/guh/extension/lifecycle/person/
├── DaccordPersonLifecycleConsumerTask.java   – Kafka consumer, routes PersonCandidate/Person/Policy events
├── PersonCandidateTaskHandler.java           – Entry/leaving/attribute-change actions on PersonCandidate events
├── PersonTaskHandler.java                    – Actions detected directly on the Person node (logging only so far)
├── PolicyTaskHandler.java                    – Time-based policy checks (leavedate, absence, deletion)
└── Main.java                                 – Standalone test runner

src/main/java/de/guh/gadget/pages/
└── customcommand.java                        – Backend for workflow form select-widgets (Gadget Core convention)

example/
├── config.xml                                – SystemDB connection template
├── test_event_*.xml                          – Sample Kafka events for standalone testing
└── processes/                                – Sample BPMN workflows (entry, name/org/absence change, ready-to-delete)
```

## License

Copyright © G+H Systems GmbH. All rights reserved.
