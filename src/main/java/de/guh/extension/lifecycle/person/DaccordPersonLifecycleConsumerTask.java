package de.guh.extension.lifecycle.person;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.neo4j.driver.Record;
import org.neo4j.driver.types.Node;

import de.guh.gadget.Gadget;
import de.guh.plugin.camunda.CamundaVariable;
import de.guh.plugin.daccord.DACConnection;
import de.guh.plugin.daccord.DACSession;
import de.guh.plugin.daccord.DACSessionException;
import de.guh.plugin.daccord.DACWorkflowEngine;
import de.guh.plugin.daccordevent.EventCollectionDTO;
import de.guh.plugin.daccordevent.EventDTO;
import de.guh.plugin.daccordevent.PropertyEventDTO;
import de.guh.plugin.kafka.KafkaConsumerTask;
import de.guh.plugin.neo4j.Graph;

/**
 * Lifecycle Consumer Task for PersonCandidate, Person and Policy Events
 * Processes lifecycle events and triggers appropriate workflows.
 * Uses optimized event processing with direct EventMap access and batch
 * database loading.
 * 
 * @author Thomas Gertler
 * @version 2.0 - Optimized Event Processing
 */
public class DaccordPersonLifecycleConsumerTask extends KafkaConsumerTask {

	// ========================================
	// MAIN EXECUTION
	// ========================================

	@Override
	public void execute(Object events) {

		// Initialize logger (same pattern as original code)
		if (Gadget.log == null) {
			Gadget.log = (Logger) getData("log");
		}

		// Get DACConnection
		DACConnection dacconnection = (DACConnection) this.getData("dacconnection");
		if (dacconnection == null || !dacconnection.isReady()) {
			Gadget.log.error("DaccordPersonLifecycleConsumerTask()...dacconnection is not ready!");
			return;
		}

		Graph database = dacconnection.dacsystemdb().db();
		DACSession dacsession;
		try {
			dacsession = new DACSession(dacconnection, "1", DACSession.verificationcode);
			DACWorkflowEngine workflowEngine = new DACWorkflowEngine(dacsession, "1", null);
			Gadget.log.debug("DaccordPersonLifecycleConsumerTask()...dacconnection is ready.");

			// Validate event type
			if (!events.getClass().getName().equalsIgnoreCase("de.guh.plugin.daccordevent.EventCollectionDTO")) {
				Gadget.log.warn("DaccordPersonLifecycleConsumerTask()...received non-EventCollectionDTO object, ignoring.");
				return;
			}

			EventCollectionDTO eventCollection = (EventCollectionDTO) events;

			Gadget.log.info("===========================================");
			Gadget.log.info("Lifecycle Event Processing Started");
			Gadget.log.info("===========================================");
			Gadget.log.info("Total events received: {}", eventCollection.getEvents().size());
			Gadget.log.info("Stream: {} (ID: {})", eventCollection.getStreamname(), eventCollection.getStreamid());
			Gadget.log.info("DB User: {}", eventCollection.getDBuser());

			// ========================================
			// GET EVENTS BY LABEL (Direct Map Access!)
			// ========================================

			Map<String, Set<EventDTO>> eventMap = eventCollection.getEventMap();
			Gadget.log.debug("EventMap contains {} label groups", eventMap.size());

			// Log all labels
			for (String label : eventMap.keySet()) {
				Gadget.log.debug("  Label: {} - Events: {}", label, eventMap.get(label).size());
			}

			// PersonCandidate Events
			Set<EventDTO> personCandidateEvents = eventMap.get("PersonCandidate");
			if (personCandidateEvents != null && !personCandidateEvents.isEmpty()) {
				Gadget.log.info("-------------------------------------------");
				Gadget.log.info("Processing PersonCandidate Events: {}", personCandidateEvents.size());
				Gadget.log.info("-------------------------------------------");
				processPersonCandidateEvents(new HashSet<>(personCandidateEvents), database, workflowEngine);
			} else {
				Gadget.log.info("No PersonCandidate events to process.");
			}

			// Person Events
			Set<EventDTO> personEvents = eventMap.get("Person");
			if (personEvents != null && !personEvents.isEmpty()) {
				Gadget.log.info("-------------------------------------------");
				Gadget.log.info("Processing Person Events: {}", personEvents.size());
				Gadget.log.info("-------------------------------------------");
				processPersonEvents(new HashSet<>(personEvents), database, workflowEngine);
			} else {
				Gadget.log.info("No Person events to process.");
			}

			// Policy Events
			Set<EventDTO> policyEvents = eventMap.get("PolicyStatistics");
			if (policyEvents != null && !policyEvents.isEmpty()) {
				Gadget.log.info("-------------------------------------------");
				Gadget.log.info("Processing Policy Events: {}", policyEvents.size());
				Gadget.log.info("-------------------------------------------");
				processPolicyEvents(new HashSet<>(policyEvents), database, workflowEngine, dacsession);
			} else {
				Gadget.log.info("No Policy events to process.");
			}

			Gadget.log.info("===========================================");
			Gadget.log.info("Lifecycle Event Processing Completed");
			Gadget.log.info("===========================================");
		} catch (DACSessionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// ========================================
	// PROCESS PERSONCANDIDATE EVENTS
	// ========================================

	private void processPersonCandidateEvents(Set<EventDTO> events, Graph database, DACWorkflowEngine workflowEngine) {

		Gadget.log.debug("processPersonCandidateEvents()...processing {} events", events.size());

		// ========================================
		// 1. COLLECT EVENT IDs BY ACTION TYPE
		// ========================================

		Set<String> allEventIds = new HashSet<>();
		Set<String> entryEventIds = new HashSet<>();
		Set<String> leavingEventIds = new HashSet<>();
		Set<String> changeOUEventIds = new HashSet<>();
		Set<String> changeNameEventIds = new HashSet<>();
		Set<String> changeRespEventIds = new HashSet<>();
		Set<String> changeAbsenceEventIds = new HashSet<>();
		Set<String> modifyEventIds = new HashSet<>();
		Set<String> deleteEventIds = new HashSet<>();

		int eventCounter = 1;

		for (EventDTO event : events) {

			String eventId = event.getSourcenodeUniqueid();
			String eventUniqueid = event.getEventUniqueid();

			Gadget.log.debug("-------------------------------------------");
			Gadget.log.debug("Event {}/{}: {}", eventCounter++, events.size(), eventUniqueid);
			Gadget.log.debug("  PersonCandidate ID: {}", eventId);
			Gadget.log.debug("  EventType: {}", event.getType());
			Gadget.log.debug("  Labels: {}", event.getLabels());
			Gadget.log.debug("  Timestamp: {}", event.getDatetime());

			allEventIds.add(eventId);

			// ========================================
			// CREATE_NODE treated as ENTRY if relevantforadd = true
			// ========================================

			if (event.getType() == EventDTO.EventType.CREATE_NODE) {

				Gadget.log.debug("  Type: CREATE_NODE");

				PropertyEventDTO relevantforadd = event.getPropertyEvent("relevantforadd");
				if (relevantforadd != null) {
					String newValue = relevantforadd.getNewvalue();
					Gadget.log.debug("  Property: relevantforadd = {}", newValue);

					if ("true".equals(newValue)) {
						Gadget.log.info("  → Action: ENTRY (CREATE with relevantforadd=true)");
						entryEventIds.add(eventId);
					} else {
						Gadget.log.debug("  → Action: NONE (CREATE but relevantforadd=false)");
					}
				} else {
					Gadget.log.debug("  → Action: NONE (CREATE but no relevantforadd property)");
				}
			}

			// ========================================
			// MODIFY_NODE: Detect action
			// ========================================

			else if (event.getType() == EventDTO.EventType.MODIFY_NODE) {

				Gadget.log.debug("  Type: MODIFY_NODE");

				// Log all changed properties
				Set<String> changedProperties = event.getPropertyNames();
				Gadget.log.debug("  Changed properties: {}", changedProperties);

				for (String propName : changedProperties) {
					PropertyEventDTO propEvent = event.getPropertyEvent(propName);
					if (propEvent != null) {
						Gadget.log.debug("    {}: {} → {}", propName, propEvent.getOldvalue(), propEvent.getNewvalue());
					}
				}

				// Detect action
				Action action = detectAction(event);

				switch (action) {
					case ENTRY:
						Gadget.log.info("  → Action: ENTRY (relevantforadd changed to true)");
						entryEventIds.add(eventId);
						break;
					case LEAVING:
						Gadget.log.info("  → Action: LEAVING (relevantfordelete changed to true)");
						leavingEventIds.add(eventId);
						break;
					case CHANGE_OU:
						Gadget.log.info("  → Action: CHANGE_OU (orgunit changed)");
						changeOUEventIds.add(eventId);
						break;
					case CHANGE_NAME:
						Gadget.log.info("  → Action: CHANGE_NAME (name changed)");
						changeNameEventIds.add(eventId);
						break;
					case CHANGE_RESP:
						Gadget.log.info("  → Action: CHANGE_RESP (manager changed)");
						changeRespEventIds.add(eventId);
						break;
					case CHANGE_ABSENCE:
						Gadget.log.info("  → Action: CHANGE_ABSENCE (absence changed)");
						changeAbsenceEventIds.add(eventId);
						break;
					case MODIFY:
						Gadget.log.info("  → Action: MODIFY (other property changes)");
						modifyEventIds.add(eventId);
						break;
				}
			}

			else if (event.getType() == EventDTO.EventType.DELETE_NODE) {

				Gadget.log.debug("  Type: DELETE_NODE");
				Gadget.log.info("  → Action: DELETE (PersonCandidate node deleted)");
				deleteEventIds.add(eventId);
			}

			else {
				Gadget.log.warn("  → Unexpected EventType: {}", event.getType());
			}
		}

		// ========================================
		// 2. LOAD DATA FROM DATABASE (BATCH!)
		// ========================================

		Gadget.log.info("-------------------------------------------");
		Gadget.log.info("Event Summary:");
		Gadget.log.info("  Total Events: {}", allEventIds.size());
		Gadget.log.info("  ENTRY: {}", entryEventIds.size());
		Gadget.log.info("  LEAVING: {}", leavingEventIds.size());
		Gadget.log.info("  CHANGE_OU: {}", changeOUEventIds.size());
		Gadget.log.info("  CHANGE_NAME: {}", changeNameEventIds.size());
		Gadget.log.info("  CHANGE_RESP: {}", changeRespEventIds.size());
		Gadget.log.info("  CHANGE_ABSENCE: {}", changeAbsenceEventIds.size());
		Gadget.log.info("  MODIFY: {}", modifyEventIds.size());
		Gadget.log.info("  DELETE: {}", deleteEventIds.size());
		Gadget.log.info("-------------------------------------------");

		Map<String, HashMap<String, String>> personCandidates = new HashMap<>();

		if (!allEventIds.isEmpty()) {
			Gadget.log.debug("Loading PersonCandidate data from database for {} IDs...", allEventIds.size());
			personCandidates = loadPersonCandidateData(database, allEventIds);
			Gadget.log.debug("Loaded data for {} PersonCandidates", personCandidates.size());
		}

		// ========================================
		// 3. PROCESS BY ACTION TYPE
		// ========================================

		// Process ENTRY events
		if (!entryEventIds.isEmpty()) {
			Gadget.log.info("===========================================");
			Gadget.log.info("Processing ENTRY Events: {}", entryEventIds.size());
			Gadget.log.info("===========================================");

			int counter = 1;
			for (String personCandidateId : entryEventIds) {
				HashMap<String, String> hmData = personCandidates.get(personCandidateId);
				if (hmData != null) {
					Gadget.log.info("ENTRY {}/{}: PersonCandidate {}", counter++, entryEventIds.size(), personCandidateId);
					try {
						PersonCandidateTaskHandler.handleEntry(hmData, workflowEngine, database);
					} catch (Exception e) {
						Gadget.log.error("Error processing ENTRY for PersonCandidate: {}", personCandidateId, e);
					}
				} else {
					Gadget.log.warn("ENTRY {}/{}: No data found for PersonCandidate {}", counter++, entryEventIds.size(), personCandidateId);
				}
			}
		}

		// Process LEAVING events
		if (!leavingEventIds.isEmpty()) {
			Gadget.log.info("===========================================");
			Gadget.log.info("Processing LEAVING Events: {}", leavingEventIds.size());
			Gadget.log.info("===========================================");

			int counter = 1;
			for (String personCandidateId : leavingEventIds) {
				HashMap<String, String> hmData = personCandidates.get(personCandidateId);
				if (hmData != null) {
					Gadget.log.info("LEAVING {}/{}: PersonCandidate {}", counter++, leavingEventIds.size(), personCandidateId);
					try {
						PersonCandidateTaskHandler.handleLeaving(hmData, workflowEngine);
					} catch (Exception e) {
						Gadget.log.error("Error processing LEAVING for PersonCandidate: {}", personCandidateId, e);
					}
				} else {
					Gadget.log.warn("LEAVING {}/{}: No data found for PersonCandidate {}", counter++, leavingEventIds.size(), personCandidateId);
				}
			}
		}

		// Process CHANGE_OU events
		if (!changeOUEventIds.isEmpty()) {
			Gadget.log.info("===========================================");
			Gadget.log.info("Processing CHANGE_OU Events: {}", changeOUEventIds.size());
			Gadget.log.info("===========================================");

			int counter = 1;
			for (String personCandidateId : changeOUEventIds) {
				HashMap<String, String> hmData = personCandidates.get(personCandidateId);
				if (hmData != null) {
					Gadget.log.info("CHANGE_OU {}/{}: PersonCandidate {}", counter++, changeOUEventIds.size(), personCandidateId);
					try {
						PersonCandidateTaskHandler.handleChangeOU(hmData, workflowEngine, database);
					} catch (Exception e) {
						Gadget.log.error("Error processing CHANGE_OU for PersonCandidate: {}", personCandidateId, e);
					}
				} else {
					Gadget.log.warn("CHANGE_OU {}/{}: No data found for PersonCandidate {}", counter++, changeOUEventIds.size(), personCandidateId);
				}
			}
		}

		// Process CHANGE_NAME events
		if (!changeNameEventIds.isEmpty()) {
			Gadget.log.info("===========================================");
			Gadget.log.info("Processing CHANGE_NAME Events: {}", changeNameEventIds.size());
			Gadget.log.info("===========================================");

			int counter = 1;
			for (String personCandidateId : changeNameEventIds) {
				HashMap<String, String> hmData = personCandidates.get(personCandidateId);
				if (hmData != null) {
					Gadget.log.info("CHANGE_NAME {}/{}: PersonCandidate {}", counter++, changeNameEventIds.size(), personCandidateId);
					try {
						PersonCandidateTaskHandler.handleChangeName(hmData, workflowEngine, database);
					} catch (Exception e) {
						Gadget.log.error("Error processing CHANGE_NAME for PersonCandidate: {}", personCandidateId, e);
					}
				} else {
					Gadget.log.warn("CHANGE_NAME {}/{}: No data found for PersonCandidate {}", counter++, changeNameEventIds.size(), personCandidateId);
				}
			}
		}

		// Process CHANGE_RESP events
		if (!changeRespEventIds.isEmpty()) {
			Gadget.log.info("===========================================");
			Gadget.log.info("Processing CHANGE_RESP Events: {}", changeRespEventIds.size());
			Gadget.log.info("===========================================");

			int counter = 1;
			for (String personCandidateId : changeRespEventIds) {
				HashMap<String, String> hmData = personCandidates.get(personCandidateId);
				if (hmData != null) {
					Gadget.log.info("CHANGE_RESP {}/{}: PersonCandidate {}", counter++, changeRespEventIds.size(), personCandidateId);
					try {
						PersonCandidateTaskHandler.handleChangeResp(hmData, workflowEngine, database);
					} catch (Exception e) {
						Gadget.log.error("Error processing CHANGE_RESP for PersonCandidate: {}", personCandidateId, e);
					}
				} else {
					Gadget.log.warn("CHANGE_RESP {}/{}: No data found for PersonCandidate {}", counter++, changeRespEventIds.size(), personCandidateId);
				}
			}
		}

		// Process CHANGE_ABSENCE events
		if (!changeAbsenceEventIds.isEmpty()) {
			Gadget.log.info("===========================================");
			Gadget.log.info("Processing CHANGE_ABSENCE Events: {}", changeAbsenceEventIds.size());
			Gadget.log.info("===========================================");

			int counter = 1;
			for (String personCandidateId : changeAbsenceEventIds) {
				HashMap<String, String> hmData = personCandidates.get(personCandidateId);
				if (hmData != null) {
					Gadget.log.info("CHANGE_ABSENCE {}/{}: PersonCandidate {}", counter++, changeAbsenceEventIds.size(), personCandidateId);
					try {
						PersonCandidateTaskHandler.handleChangeAbsence(hmData, workflowEngine, database);
					} catch (Exception e) {
						Gadget.log.error("Error processing CHANGE_ABSENCE for PersonCandidate: {}", personCandidateId, e);
					}
				} else {
					Gadget.log.warn("CHANGE_ABSENCE {}/{}: No data found for PersonCandidate {}", counter++, changeAbsenceEventIds.size(), personCandidateId);
				}
			}
		}

		// Process MODIFY events
		if (!modifyEventIds.isEmpty()) {
			Gadget.log.info("===========================================");
			Gadget.log.info("Processing MODIFY Events: {}", modifyEventIds.size());
			Gadget.log.info("===========================================");

			int counter = 1;
			for (String personCandidateId : modifyEventIds) {
				HashMap<String, String> hmData = personCandidates.get(personCandidateId);
				if (hmData != null) {
					Gadget.log.info("MODIFY {}/{}: PersonCandidate {}", counter++, modifyEventIds.size(), personCandidateId);
					try {
						PersonCandidateTaskHandler.handleModify(hmData, workflowEngine, database);
					} catch (Exception e) {
						Gadget.log.error("Error processing MODIFY for PersonCandidate: {}", personCandidateId, e);
					}
				} else {
					Gadget.log.warn("MODIFY {}/{}: No data found for PersonCandidate {}", counter++, modifyEventIds.size(), personCandidateId);
				}
			}
		}

		// Process DELETE events
		// Note: PersonCandidate is already deleted from DB at this point → no DB data
		// available.
		// Only the personCandidateId (= employeeid) from the event is used.
		if (!deleteEventIds.isEmpty()) {
			Gadget.log.info("===========================================");
			Gadget.log.info("Processing DELETE Events: {}", deleteEventIds.size());
			Gadget.log.info("===========================================");

			int counter = 1;
			for (String personCandidateId : deleteEventIds) {
				Gadget.log.info("DELETE {}/{}: PersonCandidate {}", counter++, deleteEventIds.size(), personCandidateId);
				try {
					PersonCandidateTaskHandler.handleDelete(personCandidateId, database);
				} catch (Exception e) {
					Gadget.log.error("Error processing DELETE for PersonCandidate: {}", personCandidateId, e);
				}
			}
		}
	}

	// ========================================
	// ACTION DETECTION
	// ========================================

	private enum Action {
		ENTRY, LEAVING, CHANGE_OU, CHANGE_NAME, CHANGE_RESP, CHANGE_ABSENCE, MODIFY, DELETE
	}

	private Action detectAction(EventDTO event) {

		// Get all changed properties
		Set<String> changedProperties = event.getPropertyNames();

		// Check for ENTRY: relevantforadd changed to true
		PropertyEventDTO relevantforadd = event.getPropertyEvent("relevantforadd");
		if (relevantforadd != null && "true".equals(relevantforadd.getNewvalue())) {
			return Action.ENTRY;
		}

		// Check for LEAVING: relevantfordelete changed to true
		PropertyEventDTO relevantfordelete = event.getPropertyEvent("relevantfordelete");
		if (relevantfordelete != null && "true".equals(relevantfordelete.getNewvalue())) {
			return Action.LEAVING;
		}

		// Check for CHANGE_OU: orgunit/company changes
		Set<String> orgunitProperties = Set.of("companycode", "orgunit1code", "orgunit2code", "orgunit3code");
		if (changedProperties.stream().anyMatch(orgunitProperties::contains)) {
			return Action.CHANGE_OU;
		}

		// Check for CHANGE_NAME: name changes
		if (changedProperties.contains("surname") || changedProperties.contains("givenname")) {
			return Action.CHANGE_NAME;
		}

		// Check for CHANGE_RESP: manager change
		if (changedProperties.contains("manageruniqueid")) {
			return Action.CHANGE_RESP;
		}

		// Check for CHANGE_ABSENCE: absence changes
		if (changedProperties.contains("dateofabsencestart") || changedProperties.contains("dateofabsenceend") || changedProperties.contains("systemaccess")
				|| changedProperties.contains("statusreason")) {
			return Action.CHANGE_ABSENCE;
		}

		// Default: MODIFY
		return Action.MODIFY;
	}

	// ========================================
	// DATABASE HELPER: LOAD PERSONCANDIDATE DATA (BATCH)
	// ========================================

	private Map<String, HashMap<String, String>> loadPersonCandidateData(Graph database, Set<String> guids) {

		Map<String, HashMap<String, String>> result = new HashMap<>();

		Gadget.log.debug("Loading PersonCandidate data for {} IDs...", guids.size());
		Gadget.log.debug("GUIDS: " + guids);

		// Build Cypher Query with IN clause (Batch!)
		HashMap<String, Object> parameters = new HashMap<>();
		parameters.put("guids", guids);

		String statement = "MATCH (pc:PersonCandidate) " + "WHERE pc.uniqueid IN $guids " +
			"RETURN pc";

		try {
			// Execute query - returns List<Record>
			List<Record> records = database.execute(statement, parameters);

			Gadget.log.debug("Database returned {} PersonCandidate records", records.size());

			int counter = 1;
			// Process each record
			for (Record record : records) {
				Node pcNode = record.get("pc").asNode();

				// Convert Node to HashMap
				HashMap<String, String> hmPersonCandidate = new HashMap<>();

				// Extract all properties from Node
				for (String key : pcNode.keys()) {
					if (pcNode.get(key).isNull()) {
						hmPersonCandidate.put(key, null);
					} else {
						hmPersonCandidate.put(key, pcNode.get(key).asString());
					}
				}

				String guid = hmPersonCandidate.get("uniqueid");
				result.put(guid, hmPersonCandidate);

				Gadget.log.debug("  Loaded {}/{}: {} - {} {} ({})", counter, records.size(), guid, hmPersonCandidate.get("givenname"),
						hmPersonCandidate.get("surname"), hmPersonCandidate.get("uniqueid"));

				counter++;
			}

			Gadget.log.debug("Successfully loaded data for {}/{} PersonCandidates", result.size(), guids.size());

		} catch (Exception e) {
			Gadget.log.error("Error loading PersonCandidate data from database", e);
		}

		return result;
	}

	// ========================================
	// PROCESS PERSON EVENTS
	// ========================================

	private void processPersonEvents(Set<EventDTO> events, Graph database, DACWorkflowEngine workflowEngine) {

		Gadget.log.debug("processPersonEvents()...processing {} events", events.size());

		// ========================================
		// 1. COLLECT EVENT IDs BY ACTION TYPE
		// ========================================

		Set<String> allEventIds = new HashSet<>();
		Set<String> statusToInactiveEventIds = new HashSet<>();
		Set<String> statusToAbsenceEventIds = new HashSet<>();
		Set<String> nameChangeEventIds = new HashSet<>();
		Set<String> orgChangeEventIds = new HashSet<>();
		Set<String> responsibleChangeEventIds = new HashSet<>();
		Set<String> basisdataChangeEventIds = new HashSet<>();
		Set<String> leavedateChangeEventIds = new HashSet<>();

		int eventCounter = 1;

		for (EventDTO event : events) {

			String eventId = event.getSourcenodeUniqueid();
			String eventUniqueid = event.getEventUniqueid();

			Gadget.log.debug("-------------------------------------------");
			Gadget.log.debug("Event {}/{}: {}", eventCounter++, events.size(), eventUniqueid);
			Gadget.log.debug("  Person ID: {}", eventId);
			Gadget.log.debug("  EventType: {}", event.getType());

			allEventIds.add(eventId);

			// Only process MODIFY_NODE for Person
			if (event.getType() != EventDTO.EventType.MODIFY_NODE) {
				Gadget.log.debug("  → Skipping: Not MODIFY_NODE");
				continue;
			}

			// Log changed properties
			Set<String> changedProperties = event.getPropertyNames();
			Gadget.log.debug("  Changed properties: {}", changedProperties);

			// ========================================
			// STATUS CHANGE: active → inactive
			// ========================================

			PropertyEventDTO statusEvent = event.getPropertyEvent("status");
			if (statusEvent != null) {
				String oldStatus = statusEvent.getOldvalue();
				String newStatus = statusEvent.getNewvalue();

				if ("active".equals(oldStatus) && "inactive".equals(newStatus)) {
					Gadget.log.info("  → Action: STATUS_TO_INACTIVE");
					statusToInactiveEventIds.add(eventId);
					continue;
				}

				if ("active".equals(oldStatus) && "absence".equals(newStatus)) {
					Gadget.log.info("  → Action: STATUS_TO_ABSENCE");
					statusToAbsenceEventIds.add(eventId);
					continue;
				}
			}

			// ========================================
			// NAME CHANGE
			// ========================================

			boolean hasNameChange =
					changedProperties.contains("firstname") || changedProperties.contains("lastname") || changedProperties.contains("displayname");

			if (hasNameChange) {
				Gadget.log.info("  → Action: NAME_CHANGE");
				nameChangeEventIds.add(eventId);
				continue;
			}

			// ========================================
			// ORG CHANGE
			// ========================================

			boolean hasOrgChange = changedProperties.contains("department") || changedProperties.contains("company") || changedProperties.contains("location")
					|| changedProperties.contains("orgunit");

			if (hasOrgChange) {
				Gadget.log.info("  → Action: ORG_CHANGE");
				orgChangeEventIds.add(eventId);
				continue;
			}

			// ========================================
			// RESPONSIBLE CHANGE
			// ========================================

			boolean hasResponsibleChange = changedProperties.contains("responsible") || changedProperties.contains("manager");

			if (hasResponsibleChange) {
				Gadget.log.info("  → Action: RESPONSIBLE_CHANGE");
				responsibleChangeEventIds.add(eventId);
				continue;
			}

			// ========================================
			// LEAVEDATE CHANGE
			// ========================================

			if (changedProperties.contains("leavedate")) {
				Gadget.log.info("  → Action: LEAVEDATE_CHANGE");
				leavedateChangeEventIds.add(eventId);
				continue;
			}

			// ========================================
			// BASISDATEN CHANGE
			// ========================================

			boolean hasBasisChange = changedProperties.contains("email") || changedProperties.contains("phone") || changedProperties.contains("title")
					|| changedProperties.contains("employeenumber");

			if (hasBasisChange) {
				Gadget.log.info("  → Action: BASISDATEN_CHANGE");
				basisdataChangeEventIds.add(eventId);
				continue;
			}
		}

		// ========================================
		// 2. LOAD PERSON DATA (BATCH)
		// ========================================

		Gadget.log.info("-------------------------------------------");
		Gadget.log.info("Person Event Summary:");
		Gadget.log.info("  Total: {}", allEventIds.size());
		Gadget.log.info("  STATUS_TO_INACTIVE: {}", statusToInactiveEventIds.size());
		Gadget.log.info("  STATUS_TO_ABSENCE: {}", statusToAbsenceEventIds.size());
		Gadget.log.info("  NAME_CHANGE: {}", nameChangeEventIds.size());
		Gadget.log.info("  ORG_CHANGE: {}", orgChangeEventIds.size());
		Gadget.log.info("  RESPONSIBLE_CHANGE: {}", responsibleChangeEventIds.size());
		Gadget.log.info("  BASISDATEN_CHANGE: {}", basisdataChangeEventIds.size());
		Gadget.log.info("  LEAVEDATE_CHANGE: {}", leavedateChangeEventIds.size());
		Gadget.log.info("-------------------------------------------");

		Map<String, HashMap<String, String>> personData = new HashMap<>();

		if (!allEventIds.isEmpty()) {
			personData = loadPersonData(database, allEventIds);
			Gadget.log.info("Loaded data for {} Persons", personData.size());
		}

		// ========================================
		// 3. PROCESS ACTIONS
		// ========================================

		if (!statusToInactiveEventIds.isEmpty()) {
			PersonTaskHandler.processStatusToInactiveActions(statusToInactiveEventIds, personData);
		}

		if (!statusToAbsenceEventIds.isEmpty()) {
			PersonTaskHandler.processStatusToAbsenceActions(statusToAbsenceEventIds, personData);
		}

		if (!nameChangeEventIds.isEmpty()) {
			PersonTaskHandler.processNameChangeActions(nameChangeEventIds, personData);
		}

		if (!orgChangeEventIds.isEmpty()) {
			PersonTaskHandler.processOrgChangeActions(orgChangeEventIds, personData);
		}

		if (!responsibleChangeEventIds.isEmpty()) {
			PersonTaskHandler.processResponsibleChangeActions(responsibleChangeEventIds, personData);
		}

		if (!basisdataChangeEventIds.isEmpty()) {
			PersonTaskHandler.processBasisdataChangeActions(basisdataChangeEventIds, personData);
		}

		if (!leavedateChangeEventIds.isEmpty()) {
			PersonTaskHandler.processLeavedateChangeActions(leavedateChangeEventIds, personData);
		}
	}

	// ========================================
	// PERSON: LOAD DATA
	// ========================================

	private Map<String, HashMap<String, String>> loadPersonData(Graph database, Set<String> guids) {

		Map<String, HashMap<String, String>> result = new HashMap<>();

		String statement = "MATCH (p:Person) WHERE p.globaluniqueid IN $guids RETURN p";

		HashMap<String, Object> params = new HashMap<>();
		params.put("guids", new ArrayList<>(guids));

		try {
			List<Record> records = database.execute(statement, params);

			for (Record record : records) {
				Node node = record.get("p").asNode();
				HashMap<String, String> nodeData = new HashMap<>();

				node.keys().forEach(key -> {
					nodeData.put(key, node.get(key).isNull() ? null : node.get(key).asString());
				});

				result.put(nodeData.get("globaluniqueid"), nodeData);
			}
		} catch (Exception e) {
			Gadget.log.error("Error loading Person data", e);
		}

		return result;
	}

	// ========================================
	// PROCESS POLICY EVENTS
	// ========================================

	private void processPolicyEvents(Set<EventDTO> events, Graph database, DACWorkflowEngine workflowEngine, DACSession dacsession) {

		Gadget.log.info("processPolicyEvents()...processing {} Policy events", events.size());

		for (EventDTO event : events) {

			String policyId = event.getSourcenodeUniqueid();

			Gadget.log.info("-------------------------------------------");
			Gadget.log.info("Policy Event: {}", policyId);
			Gadget.log.info("Policy Event: {}", event.toString());
			Gadget.log.info("  EventType: {}", event.getType());
			Gadget.log.info("  Timestamp: {}", event.getDatetime());

			// Resolve policy name from DB so routing is independent of policy IDs
			String policyName = "";
			try {
				HashMap<String, Object> policyParams = new HashMap<>();
				policyParams.put("policyId", policyId);
				List<org.neo4j.driver.Record> policyRecords = database.execute("MATCH (p:Policy {policyid: $policyId}) RETURN p.name AS name", policyParams);
				if (!policyRecords.isEmpty()) {
					policyName = policyRecords.get(0).get("name").asString();
				}
			} catch (de.guh.plugin.neo4j.exception.GraphException e) {
				Gadget.log.error("  → Error resolving policy name for id {}: {}", policyId, e.getMessage(), e);
			}
			Gadget.log.info("  Policy Name: {}", policyName.isEmpty() ? "(not found)" : policyName);

			// Route to appropriate policy handler by name
			try {
				switch (policyName) {
					case "check leavedate for deactivation":
						PolicyTaskHandler.handlePolicyExpiredLeavedate(database, workflowEngine, dacsession, policyId, policyName);
						break;

					case "check leavedate for readytodelete":
						PolicyTaskHandler.handlePolicyPersonLeavingReadyToDelete(database, workflowEngine, dacsession);
						break;

					case "check leavedate for deletionprocess":
						PolicyTaskHandler.handlePolicyPersonDeletionStart(database, workflowEngine, dacsession, policyId, policyName);
						break;

					case "check absencestartdate for absencestart":
						PolicyTaskHandler.handlePolicyPersonAbsenceStartdate(database, workflowEngine, dacsession);
						break;

					case "check absenceenddate for absenceend":
						PolicyTaskHandler.handlePolicyPersonAbsenceEnddate(database, workflowEngine, dacsession);
						break;
					case "check leavedate for finaldeletion":
						PolicyTaskHandler.handlePolicyPersonDeletion(database, workflowEngine, dacsession);
						break;

					default:
						Gadget.log.warn("  → Unknown Policy: {} (id: {})", policyName, policyId);
				}
			} catch (Exception e) {
				Gadget.log.error("Error processing Policy: {}", policyId, e);
			}
		}
	}

	// ========================================
	// WORKFLOW EXECUTION (COMMENTABLE!)
	// ========================================

	public static void startWorkflow(String processKey, CamundaVariable[] variables, DACWorkflowEngine workflowEngine) {

		Gadget.log.info("    ▶ Starting Workflow: {}", processKey);
		Gadget.log.info("    Variables: {} items", variables.length);

		// UNCOMMENT FOR PRODUCTION:
		// workflowEngine.startProcess(processKey, variables);

		// Gadget.log.info(" ✓ Workflow started successfully");

		// FOR TESTING: Just log
		// Gadget.log.warn(" ⚠ Workflow start COMMENTED OUT for testing!");
	}
}