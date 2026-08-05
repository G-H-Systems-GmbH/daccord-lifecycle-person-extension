package de.guh.extension.lifecycle.person;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Record;

import de.guh.gadget.Gadget;
import de.guh.plugin.camunda.CamundaAction;
import de.guh.plugin.camunda.CamundaException;
import de.guh.plugin.camunda.CamundaResponse;
import de.guh.plugin.camunda.CamundaSession;
import de.guh.plugin.camunda.CamundaSystem;
import de.guh.plugin.camunda.CamundaVariable;
import de.guh.plugin.daccord.DACSession;
import de.guh.plugin.daccord.DACWorkflowEngine;
import de.guh.plugin.daccord.extensions.lifecycle.base.DACActionLifecycle;
import de.guh.plugin.daccord.extensions.lifecycle.dac.DACLifecycleProvisioningHelper;
import de.guh.plugin.daccord.extensions.lifecycle.provisioning.RequestedAccountTask;
import de.guh.plugin.neo4j.Graph;
import de.guh.plugin.neo4j.actionobject.ActionInitiatorObject;
import de.guh.plugin.neo4j.actionobject.RequestInitiatorObject;
import de.guh.plugin.neo4j.actionobject.RequestTargetObject;
import de.guh.plugin.neo4j.exception.GraphException;

public class PolicyTaskHandler {

	// ========================================
	// POLICY 1: Expired leavedate
	// ========================================
	public static void handlePolicyExpiredLeavedate(Graph database, DACWorkflowEngine workflowEngine, DACSession dacsession, String policyId, String policyName) {

		Gadget.log.info("===========================================");
		Gadget.log.info("POLICY: Expired Leavedate");
		Gadget.log.info("===========================================");
		Gadget.log.info("Purpose: Find persons whose leavedate has passed");
		Gadget.log.info("         Set Person status = inactive");

		// ========================================
		// CYPHER QUERY (documented, not executed)
		// ========================================

		String selectStatement =
				"MATCH (p:Person {status:\"active\"}) WHERE datetime(replace(p.leavedate, ' ', 'T')) < datetime() RETURN p.globaluniqueid AS guid";
		String updateStatement = "MATCH (p:Person {globaluniqueid: $guid}) SET p.status = 'inactive'";

		// ========================================
		// 1. Find persons with expired leavedate
		// ========================================

		List<String> guids = new ArrayList<>();
		try {
			List<Record> records = database.execute(selectStatement, new HashMap<>());
			for (Record record : records) {
				String guid = record.get("guid").asString();
				guids.add(guid);
			}
		} catch (GraphException e) {
			Gadget.log.error("PolicyTaskHandler...handlePolicyExpiredLeavedate()...error reading persons: {}", e.getMessage(), e);
			return;
		}

		Gadget.log.info("Found {} person(s) with expired leavedate", guids.size());

		if (guids.isEmpty()) {
			Gadget.log.info("No persons to process.");
			return;
		}

		// ========================================
		// 2. Set each person to inactive
		// ========================================

		int successCount = 0;
		int errorCount = 0;

		DACActionLifecycle dacaction = new DACActionLifecycle(dacsession, "de");
		DACLifecycleProvisioningHelper helper = new DACLifecycleProvisioningHelper(dacaction, "de");

		ActionInitiatorObject actionInitiator = new ActionInitiatorObject(policyId, policyName, "policyid", "Policy");
		RequestInitiatorObject requestInitiator = new RequestInitiatorObject(policyId, policyName, "policyid", "Policy");

		for (String guid : guids) {
			Gadget.log.info("  Setting inactive: {}", guid);
			try {
				HashMap<String, Object> params = new HashMap<>();
				params.put("guid", guid);
				database.execute(updateStatement, params);
				Gadget.log.info("  → status = inactive ✓");
				successCount++;
			} catch (GraphException e) {
				Gadget.log.error("  → error setting inactive for {}: {}", guid, e.getMessage(), e);
				errorCount++;
				continue;
			}

			// -------------------------------TESTEN
			HashMap<String, String> accountAttributes = new HashMap<>();
			accountAttributes.put("request_status", "inactive");
			try {
				HashMap<String, String> personProps = database.getNodeAllPropertiesStringHashMap("Person", "globaluniqueid", guid);
				accountAttributes.put("request_givenname", personProps.get("givenname") != null ? personProps.get("givenname") : "");
				accountAttributes.put("request_surname", personProps.get("surname") != null ? personProps.get("surname") : "");

			} catch (GraphException e) {
				Gadget.log.warn("  → could not get person properties for account attributes: {}", e.getMessage());
			}
			RequestTargetObject requestTarget = new RequestTargetObject(guid, "globaluniqueid", "Person", database);
			helper.createRequestedAccountTaskForAllAccounts(guid, RequestedAccountTask.Action.MODIFYSTATUS, RequestedAccountTask.Status.OPEN,
					accountAttributes, actionInitiator, requestInitiator, requestTarget);
			// createRequestedAccountTaskForAllAccounts(String personGUID,
			// RequestedAccountTask.Action action, RequestedAccountTask.Status status,
			// Map<String, String> objectProperties, ActionInitiatorObject
			// actionInitiatorObject, RequestInitiatorObject requestInitiatorObject,
			// RequestTargetObject requestTargetObject)
			// ------------------------------
		}

		Gadget.log.info("-------------------------------------------");
		Gadget.log.info("Expired Leavedate Policy done: {} updated, {} errors", successCount, errorCount);
		Gadget.log.info("-------------------------------------------");
	}

	// ========================================
	// POLICY 2: PERSON_LEAVING_READY_TO_DELETE
	// ========================================
	public static void handlePolicyPersonLeavingReadyToDelete(Graph database, DACWorkflowEngine workflowEngine, DACSession dacsession) {

		Gadget.log.info("===========================================");
		Gadget.log.info("POLICY: Person readytodelete Start");
		Gadget.log.info("===========================================");
		Gadget.log.info("Purpose: Find persons ready for readytodelete notification (14 days passed)");

		String selectStatement = "MATCH (p:Person) " + "WITH p, datetime(replace(p.leavedate, ' ', 'T')) AS leavedate " +
			"WHERE p.status = 'inactive' " +
			"  AND leavedate < (datetime() - duration({days: 14})) " +
			"RETURN p.globaluniqueid AS guid";

		String updateStatement = "MATCH (p:Person {globaluniqueid: $guid}) SET p.status = 'readytodelete'";

		// ========================================
		// 1. Find persons
		// ========================================

		List<String> guids = new ArrayList<>();
		try {
			List<Record> records = database.execute(selectStatement, new HashMap<>());
			for (Record record : records) {
				guids.add(record.get("guid").asString());
			}
		} catch (GraphException e) {
			Gadget.log.error("PolicyTaskHandler...handlePolicyPersonDeletionStart()...error reading persons: {}", e.getMessage(), e);
			return;
		}

		Gadget.log.info("Found {} person(s) with 14 days expired leavedate", guids.size());

		if (guids.isEmpty()) {
			Gadget.log.info("No persons to process.");
			return;
		}

		// ========================================
		// 2. Get dAccord system ID (initiator)
		// ========================================

		String initiatorSystemId = "1";
		try {
			List<Record> sysRecords =
					database.execute("MATCH (d:Daccord)-[:IS_AUTHORIZATIONSYSTEM]->(s:System) RETURN s.systemid AS systemid", new HashMap<>());
			if (!sysRecords.isEmpty()) {
				initiatorSystemId = sysRecords.get(0).get("systemid").asString();
			}
		} catch (GraphException e) {
			Gadget.log.warn("Could not get system ID, using default '1': {}", e.getMessage());
		}

		// ========================================
		// 3. Process each person
		// ========================================

		int successCount = 0;
		int errorCount = 0;

		String camundaurl = workflowEngine.camundaurl();

		for (String guid : guids) {
			Gadget.log.info("  Processing: {}", guid);

			// Get person properties
			String givenname = "";
			String surname = "";
			String leavedate = "";
			String employeeid = "";
			try {
				HashMap<String, String> person = database.getNodeAllPropertiesStringHashMap("Person", "globaluniqueid", guid);
				givenname	= person.get("givenname") != null ? person.get("givenname") : "";
				surname		= person.get("surname") != null ? person.get("surname") : "";
				leavedate	= person.get("leavedate") != null ? person.get("leavedate") : "";
				employeeid	= person.get("employeeid") != null ? person.get("employeeid") : "";
			} catch (GraphException e) {
				Gadget.log.error("  → error reading person properties for {}: {}", guid, e.getMessage(), e);
				errorCount++;
				continue;
			}

			// Get responsible person via IS_PERSONMANAGER
			String responsibleglobaluniqueid = "";
			try {
				HashMap<String, Object> mgParams = new HashMap<>();
				mgParams.put("guid", guid);
				List<Record> mgRecords = database.execute(
						"MATCH (m:Person)-[:IS_PERSONMANAGER]->(p:Person {globaluniqueid: $guid}) RETURN m.globaluniqueid AS manager_guid LIMIT 1", mgParams);
				if (!mgRecords.isEmpty()) {
					responsibleglobaluniqueid = mgRecords.get(0).get("manager_guid").asString();
				}
				Gadget.log.info("  → responsible (IS_PERSONMANAGER): {}", responsibleglobaluniqueid);
			} catch (GraphException e) {
				Gadget.log.warn("  → could not get manager for {}: {}", guid, e.getMessage());
			}

			// Set status to readytodelete
			try {
				HashMap<String, Object> params = new HashMap<>();
				params.put("guid", guid);
				database.execute(updateStatement, params);
				Gadget.log.info("  → status = readytodelete ✓");
			} catch (GraphException e) {
				Gadget.log.error("  → error setting readytodelete for {}: {}", guid, e.getMessage(), e);
				errorCount++;
				continue;
			}

			// Build workflow variables
			HashMap<String, String> startProcessVariables = new HashMap<>();
			startProcessVariables.put("givenname", givenname);
			startProcessVariables.put("surname", surname);
			startProcessVariables.put("leavedate", leavedate);
			startProcessVariables.put("uniqueid", employeeid);
			startProcessVariables.put("responsibleglobaluniqueid", responsibleglobaluniqueid);
			startProcessVariables.put("actioninitiatorobjecttype", "System");
			startProcessVariables.put("actioninitiatoruniqueidpropertyname", "systemid");
			startProcessVariables.put("actioninitiatoruniqueid", initiatorSystemId);
			startProcessVariables.put("processinitiatorobjecttype", "System");
			startProcessVariables.put("processinitiatoruniqueidpropertyname", "systemid");
			startProcessVariables.put("processinitiatoruniqueid", initiatorSystemId);
			startProcessVariables.put("processtargetobjecttype", "Person");
			startProcessVariables.put("processtargetuniqueidpropertyname", "globaluniqueid");
			startProcessVariables.put("processtargetuniqueid", guid);
			startProcessVariables.put("system_requestsgadgetprotocol", Gadget.PROTOCOL);
			startProcessVariables.put("system_requestsgadgethost", Gadget.HOST);
			startProcessVariables.put("system_requestsgadgetport", Gadget.PORT);
			startProcessVariables.put("system_requestsgadgetpath", Gadget.PATH);
			startProcessVariables.put("system_requestsgadgetusername", "daccord");
			startProcessVariables.put("system_requestsgadgetpassword", "daccord");
			startProcessVariables.put("workflowengineid", "1");

			List<CamundaVariable> variablesList = new ArrayList<>();
			for (Map.Entry<String, String> entry : startProcessVariables.entrySet()) {
				variablesList.add(new CamundaVariable(entry.getKey(), entry.getValue(), CamundaVariable.type_STRING));
			}
			CamundaVariable[] variables = variablesList.toArray(new CamundaVariable[0]);

			// Start process
			Gadget.log.info("  → starting process person-readytodelete ({} variables)", variables.length);
			CamundaSystem camundasystem = new CamundaSystem(camundaurl);
			try {
				CamundaSession session = new CamundaSession(camundasystem, "admin", "admin");
				CamundaAction action = new CamundaAction(session);
				CamundaResponse gr = action.startProcess("person-readytodelete", variables);
				if (gr.wasSuccess()) {
					Gadget.log.info("  → process started ✓");
					successCount++;
				} else {
					Gadget.log.error("  → process error: {}", gr.getStatusmessage());
					Gadget.log.error("  → process error XML: {}", gr.getStatusXML());
					errorCount++;
				}
			} catch (CamundaException e) {
				Gadget.log.error("  → camunda error for {}: {}", guid, e.getMessage(), e);
				errorCount++;
			}
		}

		Gadget.log.info("-------------------------------------------");
		Gadget.log.info("Person readytodelete Policy done: {} processed, {} errors", successCount, errorCount);
		Gadget.log.info("-------------------------------------------");
	}

	// ========================================
	// POLICY 3: PERSON_Deletion
	// ========================================

	public static void handlePolicyPersonDeletionStart(Graph database, DACWorkflowEngine workflowEngine, DACSession dacsession, String policyId, String policyName) {

		Gadget.log.info("===========================================");
		Gadget.log.info("POLICY: Person Deletion in Progress");
		Gadget.log.info("===========================================");

		// ========================================
		// CYPHER QUERY (documented, not executed)
		// ========================================

		String selectStatement = "MATCH (p:Person) " + "WITH p, datetime(replace(p.leavedate, ' ', 'T')) AS leavedate " +
			"WHERE p.status = 'readytodelete' " +
			"  AND leavedate < (datetime() - duration({days: 28})) " +
			"RETURN p.globaluniqueid AS guid";

		String updateStatement = "MATCH (p:Person {globaluniqueid: $guid}) SET p.status = 'deletioninprogress'";

		Gadget.log.info("Query (Select):\n{}", selectStatement);

		List<String> guids = new ArrayList<>();
		try {
			List<Record> records = database.execute(selectStatement, new HashMap<>());
			for (Record record : records) {
				String guid = record.get("guid").asString();
				guids.add(guid);
			}
		} catch (GraphException e) {
			Gadget.log.error("PolicyTaskHandler...handlePolicyDeletionInProgress()...error reading persons: {}", e.getMessage(), e);
			return;
		}

		Gadget.log.info("Found {} person(s) with expired leavedate", guids.size());

		if (guids.isEmpty()) {
			Gadget.log.info("No persons to process.");
			return;
		}

		// ========================================
		// 2. Set each person to deletioninprogress
		// ========================================

		int successCount = 0;
		int errorCount = 0;

		DACActionLifecycle dacaction = new DACActionLifecycle(dacsession, "de");
		DACLifecycleProvisioningHelper helper = new DACLifecycleProvisioningHelper(dacaction, "de");

		ActionInitiatorObject actionInitiator = new ActionInitiatorObject(policyId, policyName, "policyid", "Policy");
		RequestInitiatorObject requestInitiator = new RequestInitiatorObject(policyId, policyName, "policyid", "Policy");

		for (String guid : guids) {
			Gadget.log.info("  Setting deletioninprogress: {}", guid);
			try {
				HashMap<String, Object> params = new HashMap<>();
				params.put("guid", guid);
				database.execute(updateStatement, params);
				Gadget.log.info("  → status = deletioninprogress ✓");
				successCount++;
			} catch (GraphException e) {
				Gadget.log.error("  → error setting deletioninprogress for {}: {}", guid, e.getMessage(), e);
				errorCount++;
				continue;
			}

			HashMap<String, String> accountAttributes = new HashMap<>();
			try {
				HashMap<String, String> personProps = database.getNodeAllPropertiesStringHashMap("Person", "globaluniqueid", guid);
				accountAttributes.put("object_givenname", personProps.get("givenname") != null ? personProps.get("givenname") : "");
				accountAttributes.put("object_surname", personProps.get("surname") != null ? personProps.get("surname") : "");
			} catch (GraphException e) {
				Gadget.log.warn("  → could not get person properties for account attributes: {}", e.getMessage());
			}
			RequestTargetObject requestTarget = new RequestTargetObject(guid, "globaluniqueid", "Person", database);
			helper.createRequestedAccountTaskForAllAccounts(guid, RequestedAccountTask.Action.REMOVE, RequestedAccountTask.Status.OPEN,
					accountAttributes, actionInitiator, requestInitiator, requestTarget);
			Gadget.log.info("  → createRequestedAccountTaskForAllAccounts(REMOVE) ✓");
		}

		// ========================================
		// 3. ServiceKonto — transfer IS_ACCOUNTRESPONSIBLE to manager
		// ========================================

		for (String guid : guids) {
			Gadget.log.info("  ServiceKonto transfer for: {}", guid);

			// Get manager via IS_PERSONMANAGER
			String managerGuid = "";
			try {
				HashMap<String, Object> mgParams = new HashMap<>();
				mgParams.put("guid", guid);
				List<Record> mgRecords = database.execute(
						"MATCH (m:Person)-[:IS_PERSONMANAGER]->(p:Person {globaluniqueid: $guid}) RETURN m.globaluniqueid AS manager_guid LIMIT 1", mgParams);
				if (!mgRecords.isEmpty()) {
					managerGuid = mgRecords.get(0).get("manager_guid").asString();
				}
				Gadget.log.info("  → manager: {}", managerGuid.isEmpty() ? "(none)" : managerGuid);
			} catch (GraphException e) {
				Gadget.log.warn("  → could not get manager for {}: {}", guid, e.getMessage());
			}

			if (managerGuid.isEmpty()) {
				Gadget.log.warn("  → no manager found for {}, skipping ServiceKonto transfer", guid);
				continue;
			}

			// Transfer IS_ACCOUNTRESPONSIBLE links from person to manager
			try {
				HashMap<String, Object> transferParams = new HashMap<>();
				transferParams.put("personGuid", guid);
				transferParams.put("managerGuid", managerGuid);
				List<Record> transferResult = database.execute("MATCH (p:Person {globaluniqueid: $personGuid})-[r:IS_ACCOUNTRESPONSIBLE]->(a:Account) " +
					"MATCH (m:Person {globaluniqueid: $managerGuid}) " +
					"DELETE r " +
					"MERGE (m)-[:IS_ACCOUNTRESPONSIBLE]->(a) " +
					"RETURN count(a) AS transferred", transferParams);
				int transferred = transferResult.isEmpty() ? 0 : transferResult.get(0).get("transferred").asInt();
				Gadget.log.info("  → {} account(s) transferred to manager {} ✓", transferred, managerGuid);
			} catch (GraphException e) {
				Gadget.log.error("  → error transferring accounts for {}: {}", guid, e.getMessage(), e);
			}
		}

		Gadget.log.info("-------------------------------------------");
		Gadget.log.info("Expired Leavedate 28 days deletion Policy done: {} updated, {} errors", successCount, errorCount);
		Gadget.log.info("-------------------------------------------");
	}

	// ========================================
	// POLICY 3: PERSON_Final_Deletion
	// ========================================

	public static void handlePolicyPersonDeletion(Graph database, DACWorkflowEngine workflowEngine, DACSession dacsession) {

		Gadget.log.info("===========================================");
		Gadget.log.info("POLICY: Person Final Deletion");
		Gadget.log.info("===========================================");
		Gadget.log.info("Purpose: Delete persons in status deletioninprogress with no accounts");
		Gadget.log.info("         and leavedate > 30 days in the past");

		// From policy 1002005 firstdataquery
		String selectStatement = "MATCH (p:Person) " + "WITH p, datetime(replace(p.leavedate, ' ', 'T')) AS leavedate " +
			"WHERE p.status = 'deletioninprogress' " +
			"  AND NOT (p)-[:HAS_SECURITYPRINCIPAL]->() " +
			"  AND leavedate < datetime() - duration({days: 30}) " +
			"RETURN p.globaluniqueid AS guid, p.surname AS surname, p.givenname AS givenname";

		String deleteStatement = "MATCH (p:Person {globaluniqueid: $guid}) DETACH DELETE p";

		// ========================================
		// 1. Find persons to delete
		// ========================================

		List<String> guids = new ArrayList<>();
		try {
			List<Record> records = database.execute(selectStatement, new HashMap<>());
			for (Record record : records) {
				String guid = record.get("guid").asString();
				String name = record.get("givenname").asString() + " " +
					record.get("surname").asString();
				Gadget.log.info("  Found for deletion: {} ({})", name, guid);
				guids.add(guid);
			}
		} catch (GraphException e) {
			Gadget.log.error("handlePolicyPersonDeletion...error reading persons: {}", e.getMessage(), e);
			return;
		}

		Gadget.log.info("Found {} person(s) for final deletion", guids.size());

		if (guids.isEmpty()) {
			Gadget.log.info("No persons to delete.");
			return;
		}

		// ========================================
		// 2. Delete each person
		// ========================================

		int successCount = 0;
		int errorCount = 0;

		for (String guid : guids) {
			Gadget.log.info("  Deleting: {}", guid);
			try {
				HashMap<String, Object> params = new HashMap<>();
				params.put("guid", guid);
				database.execute(deleteStatement, params);
				Gadget.log.info("  → DETACH DELETE ✓");
				successCount++;
			} catch (GraphException e) {
				Gadget.log.error("  → error deleting person {}: {}", guid, e.getMessage(), e);
				errorCount++;
			}
		}

		Gadget.log.info("-------------------------------------------");
		Gadget.log.info("Person Final Deletion Policy done: {} deleted, {} errors", successCount, errorCount);
		Gadget.log.info("-------------------------------------------");
	}

	// ========================================
	// POLICY 4: CHECK ABSENCESTART (1002003)
	// ========================================

	public static void handlePolicyPersonAbsenceStartdate(Graph database, DACWorkflowEngine workflowEngine, DACSession dacsession) {

		Gadget.log.info("===========================================");
		Gadget.log.info("POLICY: Check Absence Start");
		Gadget.log.info("===========================================");
		Gadget.log.info("Purpose: Find active persons whose absencestartdate has been reached and set status = absence");

		// From policy 1002003 firstdataquery – adapted to return globaluniqueid
		String selectStatement = "MATCH (n:Person) " + "WITH n, date(n.absencestartdate) AS absencestartdate " +
			"WHERE n.status = 'active' " +
			"  AND absencestartdate <= date() " +
			"RETURN n.globaluniqueid AS guid, n.givenname AS givenname, n.surname AS surname";

		String updateStatement = "MATCH (p:Person {globaluniqueid: $guid}) SET p.status = 'absent'";

		// ========================================
		// 1. Find persons whose absence has started
		// ========================================

		List<String> guids = new ArrayList<>();
		try {
			List<Record> records = database.execute(selectStatement, new HashMap<>());
			for (Record record : records) {
				String guid = record.get("guid").asString();
				String name = record.get("givenname").asString() + " " +
					record.get("surname").asString();
				Gadget.log.info("  Found: {} ({})", name, guid);
				guids.add(guid);
			}
		} catch (GraphException e) {
			Gadget.log.error("handlePolicyAbsenceStart...error reading persons: {}", e.getMessage(), e);
			return;
		}

		Gadget.log.info("Found {} person(s) with started absence", guids.size());

		if (guids.isEmpty()) {
			Gadget.log.info("No persons to process.");
			return;
		}

		// ========================================
		// 2. Set each person to absence
		// ========================================

		int successCount = 0;
		int errorCount = 0;

		for (String guid : guids) {
			Gadget.log.info("  Setting absence: {}", guid);
			try {
				HashMap<String, Object> params = new HashMap<>();
				params.put("guid", guid);
				database.execute(updateStatement, params);
				Gadget.log.info("  → status = absence ✓");
				successCount++;
			} catch (GraphException e) {
				Gadget.log.error("  → error setting absence for {}: {}", guid, e.getMessage(), e);
				errorCount++;
			}
		}

		Gadget.log.info("-------------------------------------------");
		Gadget.log.info("Absence Start Policy done: {} updated, {} errors", successCount, errorCount);
		Gadget.log.info("-------------------------------------------");

	}

	// ========================================
	// POLICY 5: CHECK ABSENCE ENDDATE (1002004)
	// ========================================

	public static void handlePolicyPersonAbsenceEnddate(Graph database, DACWorkflowEngine workflowEngine, DACSession dacsession) {

		Gadget.log.info("===========================================");
		Gadget.log.info("POLICY: Check Absence Enddate");
		Gadget.log.info("===========================================");
		Gadget.log.info("Purpose: Find absent persons whose absenceenddate has passed and set status = active");

		// From policy 1002004 firstdataquery – adapted to return globaluniqueid
		String selectStatement = "MATCH (n:Person) " + "WITH n, date(n.absenceenddate) AS absenceenddate " +
			"WHERE n.status = 'absent' " +
			"  AND absenceenddate < date() " +
			"RETURN n.globaluniqueid AS guid, n.givenname AS givenname, n.surname AS surname";

		String updateStatement = "MATCH (p:Person {globaluniqueid: $guid}) SET p.status = 'active' REMOVE p.absencestartdate, p.absenceenddate";

		// ========================================
		// 1. Find persons whose absence has ended
		// ========================================

		List<String> guids = new ArrayList<>();
		try {
			List<Record> records = database.execute(selectStatement, new HashMap<>());
			for (Record record : records) {
				String guid = record.get("guid").asString();
				String name = record.get("givenname").asString() + " " +
					record.get("surname").asString();
				Gadget.log.info("  Found: {} ({})", name, guid);
				guids.add(guid);
			}
		} catch (GraphException e) {
			Gadget.log.error("handlePolicyAbsenceEnddate...error reading persons: {}", e.getMessage(), e);
			return;
		}

		Gadget.log.info("Found {} person(s) with ended absence", guids.size());

		if (guids.isEmpty()) {
			Gadget.log.info("No persons to process.");
			return;
		}

		// ========================================
		// 2. Set each person back to active
		// ========================================

		int successCount = 0;
		int errorCount = 0;

		for (String guid : guids) {
			Gadget.log.info("  Setting active: {}", guid);
			try {
				HashMap<String, Object> params = new HashMap<>();
				params.put("guid", guid);
				database.execute(updateStatement, params);
				Gadget.log.info("  → status = active ✓");
				successCount++;
			} catch (GraphException e) {
				Gadget.log.error("  → error setting active for {}: {}", guid, e.getMessage(), e);
				errorCount++;
			}
		}

		Gadget.log.info("-------------------------------------------");
		Gadget.log.info("Absence Enddate Policy done: {} updated, {} errors", successCount, errorCount);
		Gadget.log.info("-------------------------------------------");
	}
}
