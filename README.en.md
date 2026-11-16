---
title: Lifecycle Extension Persons
---

# daccord Lifecycle Extension – Persons

The extension automates the **lifecycle of persons** in daccord. It processes changes from the HR import (PersonCandidate), changes to persons (Person) and events from policies (Policy) and starts the matching **workflows** in the daccord workflow engine: joining, leaving, changes of organisation, name and manager, absence, master data changes and the staged deletion.

Version 2.0 is for **daccord 2.3** and needs the IP Lifecycle of daccord 2.3 and inchorus-daccord-lifecycle-extension 2.3.3. For daccord 2.2, version 1.0 still applies.

The source code is available as a code example on GitHub: [https://github.com/G-H-Systems-GmbH/daccord-lifecycle-person-extension](https://github.com/G-H-Systems-GmbH/daccord-lifecycle-person-extension).

## Contents

- [Features](#features)
- [Installation in daccord](#installation-in-daccord)
- [Events and workflows](#events-and-workflows)
- [Policies](#policies)
- [Customising](#customising)
- [Testing](#testing)
- [Technology](#technology)
- [Building](#building)

## Features

- **PersonCandidate events** (HR import): detects joining, leaving, changes of organisation, name, manager and absence as well as other changes from the changed fields and starts one workflow per case
- **Policy events**: processes the daily checks on leave date and absence; waiting periods are kept in the database and survive restarts
- **Person events**: are detected and logged; actions are not implemented here yet
- Workflows are always started through the daccord workflow engine, with initiator and target (policy or person) passed along

## Installation in daccord

1. Copy the JAR `daccord-lifecycle-person-extension-2.0.x.jar` to `/opt/GuH/daccord-requests-gadget/res/extensions/` (owner as for the other files of the RuntimeEngine) and restart the RuntimeEngine so that the daccord-requests-gadget loads the extension.
2. Set up the event streams that use the extension as consumer – consumer class in each case `de.guh.extension.lifecycle.person.DaccordPersonLifecycleConsumerTask`:

| Stream | Content |
|---|---|
| 1011 | Changes to PersonCandidate from the HR import |
| 1012 | Policy events (topic `lifecycle_policystatistics`) |

3. Deploy the BPMN processes from [Events and workflows](#events-and-workflows) in the workflow engine (templates partly in `example/processes/`; `person-internal-add-auto`, `person-internal-delete-auto`, `person-mod-resp` and `person-mod-costresp` come from the IP Lifecycle) and create the policies from [Policies](#policies).

## Events and workflows

A PersonCandidate event is classified in this order; the first match applies:

| Action | Trigger (changed field) | Workflow |
|---|---|---|
| ENTRY | `relevantforadd` becomes `true` | `person-internal-add-auto` |
| LEAVING | `relevantfordelete` becomes `true` | `person-internal-delete-auto` |
| CHANGE_OU | `companycode`, `orgunit1code` … `orgunit3code` | `person-mod-org` |
| CHANGE_NAME | `surname`, `givenname` | `person-mod-name` |
| CHANGE_RESP | `manageruniqueid` | `person-mod-resp` and `person-mod-costresp` |
| CHANGE_ABSENCE | `dateofabsencestart`, `dateofabsenceend`, `systemaccess`, `statusreason` | `person-mod-absence-auto` |
| MODIFY | any other change (e.g. `phone`, `mobile`, `costcenter`, `position`, `jobtitle`, `leavedate`) | `person-mod-attributes-auto` |

If a PersonCandidate is deleted, the extension sets `leavedate` to today on the matching person (same `employeeid`, no leave date yet). The policy `check leavedate for deactivation` processes it on its next run.

## Policies

The policy is matched by its **name**, not by its ID:

| Policy name | Effect |
|---|---|
| `check leavedate for deactivation` | Deactivate the accounts of persons who have left |
| `check leavedate for readytodelete` | Start workflow `person-readytodelete` |
| `check leavedate for deletionprocess` | Request deletion of the accounts |
| `check absencestartdate for absencestart` | Process the start of the absence |
| `check absenceenddate for absenceend` | Process the end of the absence |
| `check leavedate for finaldeletion` | Delete the person permanently |

Unknown names are skipped with a warning in the log.

## Customising

The extension is the standard template for customer projects. The `PersonCandidateTaskHandler` defines the field names under which the values are read from the PersonCandidate; the field names come directly from the HR system and must be checked for each customer.

## Testing

Testing uses the `-runnable.jar` against a real daccord 2.3 system with IP Lifecycle. The extension connects to the SystemDB, passes the test event directly to the consumer (without Kafka) and **starts real workflows and writes to the database** – only against a test system.

```bash
java -Dlog4j2.level=DEBUG -jar daccord-lifecycle-person-extension-2.0.0-runnable.jar \
  config.xml test_event_entry.xml
```

`config.xml` needs the SystemDB connection of the system (template `example/config.xml`). Test events for all actions and policies are in `example/` (`test_event_*.xml`); adapt employee numbers and IDs to the test system first.

## Technology

| | |
|---|---|
| Language | Java 21 |
| Build | Maven, Shade plugin for the `-runnable.jar` (test) |
| Platform | daccord 2.3, inchorus-daccord-lifecycle-extension 2.3.3, gadget-core 4.x |
| Workflows | inchorus-camunda-plugin 2.0.17 (like daccord-requests-gadget and daccord-workflow-engine 2.3) |
| Logging | Log4j 2 |

## Building

```bash
mvn clean package
```
