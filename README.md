---
title: Lifecycle Extension Personen
---

# daccord Lifecycle Extension – Personen

Die Extension setzt den **Lebenszyklus von Personen** in daccord automatisch um. Sie verarbeitet Änderungen aus dem HR-Import (PersonCandidate), an Personen (Person) und Ereignisse von Richtlinien (Policy) und startet dafür die passenden **Workflows** in der daccord Workflow Engine: Eintritt, Austritt, Organisations-, Namens- und Vorgesetztenwechsel, Abwesenheit, Stammdatenänderungen und die gestufte Löschung.

Version 2.0 ist für **daccord 2.3** und braucht das IP Lifecycle aus daccord 2.3 und inchorus-daccord-lifecycle-extension 2.3.3. Für daccord 2.2 gilt weiter Version 1.0.

Der Quellcode steht als Code-Beispiel auf GitHub bereit: [https://github.com/G-H-Systems-GmbH/daccord-lifecycle-person-extension](https://github.com/G-H-Systems-GmbH/daccord-lifecycle-person-extension).

## Inhalt

- [Funktionsumfang](#funktionsumfang)
- [Installation in daccord](#installation-in-daccord)
- [Ereignisse und Workflows](#ereignisse-und-workflows)
- [Richtlinien](#richtlinien)
- [Anpassen](#anpassen)
- [Testen](#testen)
- [Technik](#technik)
- [Bauen](#bauen)

## Funktionsumfang

- **PersonCandidate-Ereignisse** (HR-Import): erkennt aus den geänderten Feldern Eintritt, Austritt, Wechsel von Organisation, Name, Vorgesetztem und Abwesenheit sowie sonstige Änderungen und startet je Fall einen Workflow
- **Richtlinien-Ereignisse**: verarbeitet die täglichen Prüfungen auf Austrittsdatum und Abwesenheit; Wartezeiten liegen so in der Datenbank und überstehen Neustarts
- **Person-Ereignisse**: werden erkannt und protokolliert; Aktionen sind hier noch nicht umgesetzt
- Workflows werden immer über die daccord Workflow Engine gestartet, Initiator und Ziel (Policy bzw. Person) werden mitgegeben

## Installation in daccord

1. Das JAR `daccord-lifecycle-person-extension-2.0.x.jar` nach `/opt/GuH/daccord-requests-gadget/res/extensions/` kopieren (Eigentümer wie die übrigen Dateien der RuntimeEngine) und die RuntimeEngine neu starten, damit das daccord-requests-gadget die Extension lädt.
2. Die Event-Streams einrichten, die die Extension als Consumer nutzen – Consumer-Klasse jeweils `de.guh.extension.lifecycle.person.DaccordPersonLifecycleConsumerTask`:

| Stream | Inhalt |
|---|---|
| 1011 | Änderungen an PersonCandidate aus dem HR-Import |
| 1012 | Richtlinien-Ereignisse (Topic `lifecycle_policystatistics`) |

3. Die BPMN-Prozesse aus [Ereignisse und Workflows](#ereignisse-und-workflows) in der Workflow Engine bereitstellen (Vorlagen teilweise in `example/processes/`; `person-internal-add-auto`, `person-internal-delete-auto`, `person-mod-resp` und `person-mod-costresp` kommen aus dem IP Lifecycle) und die Richtlinien aus [Richtlinien](#richtlinien) anlegen.

## Ereignisse und Workflows

Ein PersonCandidate-Ereignis wird in dieser Reihenfolge eingeordnet; der erste Treffer gilt:

| Aktion | Auslöser (geändertes Feld) | Workflow |
|---|---|---|
| ENTRY | `relevantforadd` wird `true` | `person-internal-add-auto` |
| LEAVING | `relevantfordelete` wird `true` | `person-internal-delete-auto` |
| CHANGE_OU | `companycode`, `orgunit1code` … `orgunit3code` | `person-mod-org` |
| CHANGE_NAME | `surname`, `givenname` | `person-mod-name` |
| CHANGE_RESP | `manageruniqueid` | `person-mod-resp` und `person-mod-costresp` |
| CHANGE_ABSENCE | `dateofabsencestart`, `dateofabsenceend`, `systemaccess`, `statusreason` | `person-mod-absence-auto` |
| MODIFY | jede andere Änderung (z.B. `phone`, `mobile`, `costcenter`, `position`, `jobtitle`, `leavedate`) | `person-mod-attributes-auto` |

Wird ein PersonCandidate gelöscht, setzt die Extension bei der zugehörigen Person (gleiche `employeeid`, noch ohne Austrittsdatum) `leavedate` auf heute. Die Richtlinie `check leavedate for deactivation` verarbeitet sie beim nächsten Lauf.

## Richtlinien

Die Richtlinie wird über ihren **Namen** zugeordnet, nicht über die ID:

| Name der Richtlinie | Wirkung |
|---|---|
| `check leavedate for deactivation` | Konten der ausgetretenen Personen deaktivieren |
| `check leavedate for readytodelete` | Workflow `person-readytodelete` starten |
| `check leavedate for deletionprocess` | Löschung der Konten beantragen |
| `check absencestartdate for absencestart` | Beginn der Abwesenheit verarbeiten |
| `check absenceenddate for absenceend` | Ende der Abwesenheit verarbeiten |
| `check leavedate for finaldeletion` | Person endgültig löschen |

Unbekannte Namen werden mit einer Warnung im Log übergangen.

## Anpassen

Die Extension ist die Standardvorlage für Kundenprojekte. Unter welchen Feldnamen die Werte aus dem PersonCandidate gelesen werden, legt der `PersonCandidateTaskHandler` fest; die Feldnamen kommen direkt aus dem HR-System und müssen je Kunde geprüft werden.

## Testen

Getestet wird mit dem `-runnable.jar` gegen ein echtes daccord-2.3-System mit IP Lifecycle. Die Extension verbindet sich mit der SystemDB, übergibt das Test-Ereignis direkt an den Consumer (ohne Kafka) und **startet echte Workflows und schreibt in die Datenbank** – nur gegen ein Testsystem.

```bash
java -Dlog4j2.level=DEBUG -jar daccord-lifecycle-person-extension-2.0.0-runnable.jar \
  config.xml test_event_entry.xml
```

`config.xml` braucht die SystemDB-Verbindung des Systems (Vorlage `example/config.xml`). Test-Ereignisse für alle Aktionen und Richtlinien liegen in `example/` (`test_event_*.xml`); Personalnummern und IDs vorher an das Testsystem anpassen.

## Technik

| | |
|---|---|
| Sprache | Java 21 |
| Build | Maven, Shade-Plugin für das `-runnable.jar` (Test) |
| Plattform | daccord 2.3, inchorus-daccord-lifecycle-extension 2.3.3, gadget-core 4.x |
| Workflows | inchorus-camunda-plugin 2.0.17 (wie daccord-requests-gadget und daccord-workflow-engine 2.3) |
| Logging | Log4j 2 |

## Bauen

```bash
mvn clean package
```
