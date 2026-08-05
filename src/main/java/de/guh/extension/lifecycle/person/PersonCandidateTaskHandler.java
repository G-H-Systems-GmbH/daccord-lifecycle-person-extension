package de.guh.extension.lifecycle.person;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.guh.gadget.Gadget;
import de.guh.plugin.camunda.CamundaAction;
import de.guh.plugin.camunda.CamundaException;
import de.guh.plugin.camunda.CamundaResponse;
import de.guh.plugin.camunda.CamundaSession;
import de.guh.plugin.camunda.CamundaSystem;
import de.guh.plugin.camunda.CamundaVariable;
import de.guh.plugin.daccord.DACConnection;
import de.guh.plugin.daccord.DACSession;
import de.guh.plugin.daccord.DACWorkflowEngine;
import org.neo4j.driver.Record;

import de.guh.plugin.neo4j.Graph;
import de.guh.plugin.neo4j.exception.GraphException;
import de.guh.plugin.xml.XML;

public class PersonCandidateTaskHandler {

	// ========================================
	// ACTION HANDLERS: LEAVING
	// ========================================

	public static void handleLeaving(HashMap<String, String> hmData, DACWorkflowEngine workflowEngine) {

		String uniqueid = hmData.get("uniqueid");
		String givenname = hmData.get("givenname");
		String surname = hmData.get("surname");

		Gadget.log.info("  LEAVING Handler:");
		Gadget.log.info("    Unique ID: {}", uniqueid);
		Gadget.log.info("    Name: {} {}", givenname, surname);

		// Build workflow variables
		CamundaVariable[] variables = buildLeavingVariables(hmData);

		Gadget.log.info("    Workflow Variables: {} variables prepared", variables.length);

		// Start workflow (commentable for testing!)
		DaccordPersonLifecycleConsumerTask.startWorkflow("person-internal-delete-auto", variables, workflowEngine);
	}

	private static CamundaVariable[] buildLeavingVariables(HashMap<String, String> hmData) {

		List<CamundaVariable> variablesList = new ArrayList<>();

		// Add relevant attributes
		variablesList.add(new CamundaVariable("uniqueid", hmData.get("uniqueid"), CamundaVariable.type_STRING));
		variablesList.add(new CamundaVariable("givenname", hmData.get("givenname"), CamundaVariable.type_STRING));
		variablesList.add(new CamundaVariable("surname", hmData.get("surname"), CamundaVariable.type_STRING));

		return variablesList.toArray(new CamundaVariable[0]);
	}

	// ========================================
	// ACTION HANDLERS: ENTRY
	// ========================================

	public static void handleEntry(HashMap<String, String> hmData, DACWorkflowEngine workflowEngine, Graph database) {
		Gadget.log.info("  ENTRY Handler:");
		String uniqueid = hmData.get("uniqueid");
		//responsible
		String manageruniqueid = hmData.get("manageruniqueid");
		String responsibleaccountname = getAccountById(database, manageruniqueid);
		
		//person
		String givenname = hmData.get("givenname");
		String surname = hmData.get("surname");
		String gender = hmData.get("gender");
		String salutation ="";
		if ("M".equals(gender))
			salutation = "Herr";
		else if ("F".equals(gender))
			salutation = "Frau";
		String title = hmData.get("title");
		String jobtitle = hmData.get("jobtitle");
		String costcenter = hmData.get("costcenter");
		String phone = hmData.get("phone");
		String mobile = hmData.get("mobile");
		
		String locationcode = hmData.get("locationcode");
		String companycode = hmData.get("companycode");
		String orgunit1code = hmData.get("orgunit1code");
		String orgunit2code = hmData.get("orgunit2code");
		String orgunit3code = hmData.get("orgunit3code");
		String language="";
		String orgunit1globaluniqueid="";
		String orgunit2globaluniqueid="";
		String orgunit3globaluniqueid="";
		String companyglobaluniqueid="";
		String locationglobaluniqueid="";
		String orgunit1name="";
		String orgunit2name="";
		String orgunit3name="";
		String companyname="";
		String locationname="";
		String countrylong ="";
		String countrycodenumeric="";
		String street="";
		String postalcode="";
		String city="";
		String state="";
		String responsibleglobaluniqueid="";
		String costresponsibleglobaluniqueid="";
		String responsiblename="";
		String costresponsiblename="";
		try {
			HashMap<String, String> location = database.getNodeAllPropertiesStringHashMap("Location", "code", locationcode);
			HashMap<String, String> company = database.getNodeAllPropertiesStringHashMap("Company", "code", companycode);
			HashMap<String, String> orgunit1 = (orgunit1code != null && !orgunit1code.isEmpty())
					? database.getNodeAllPropertiesStringHashMap("Orgunit", "code", orgunit1code)
					: new HashMap<>();
			HashMap<String, String> orgunit2 = (orgunit2code != null && !orgunit2code.isEmpty())
					? database.getNodeAllPropertiesStringHashMap("Orgunit", "code", orgunit2code)
					: new HashMap<>();
			HashMap<String, String> orgunit3 = (orgunit3code != null && !orgunit3code.isEmpty())
					? database.getNodeAllPropertiesStringHashMap("Orgunit", "code", orgunit3code)
					: new HashMap<>();
			HashMap<String, String> manager = database.getNodeAllPropertiesStringHashMap("Person", "uniqueid", manageruniqueid);
			language = location.get("language");
			orgunit1globaluniqueid = orgunit1.get("globaluniqueid");
			orgunit1name = orgunit1.get("name");
			orgunit2globaluniqueid = orgunit2.get("globaluniqueid");
			orgunit2name = orgunit2.get("name");
			orgunit3globaluniqueid = orgunit3.get("globaluniqueid");
			orgunit3name = orgunit3.get("name");
			companyglobaluniqueid = company.get("globaluniqueid");
			companyname = company.get("name");
			locationglobaluniqueid = location.get("globaluniqueid");
			locationname = location.get("name");
			countrylong = location.get("country");
			countrycodenumeric = location.get("countrycodenumeric");
			street = location.get("street");
			postalcode = location.get("postalcode");
			city = location.get("city");
			state = location.get("state");
			responsibleglobaluniqueid = manager.get("globaluniqueid");
			costresponsibleglobaluniqueid = manager.get("globaluniqueid"); //Identisch zu responsible -> Da nur einer von HR kommt
			responsiblename = manager.get("givenname") + " "+ manager.get("surname");
			costresponsiblename = manager.get("givenname") + " "+ manager.get("surname");
			
		} catch (GraphException e) {
			Gadget.log.error("Error loading related data for PersonCandidate {}: {}", uniqueid, e.getMessage(), e);
		}
		
		String systemidsForUniqueAccountname = getAuthorisationSystemID(database);
		String systemid = getSystembyObject(database,"Person","globaluniqueid",responsibleglobaluniqueid);
		
		
		Gadget.log.info("  uniqueid:" + uniqueid);
		Gadget.log.info("  manageruniqueid:" + manageruniqueid);
		Gadget.log.info("  responsibleaccountname:" + responsibleaccountname);
		Gadget.log.info("  givenname:" + givenname);
		Gadget.log.info("  surname:" + surname);
		Gadget.log.info("  gender:" + gender);
		Gadget.log.info("  salutation:" + salutation);
		Gadget.log.info("  title:" + title);
		Gadget.log.info("  jobtitle:" + jobtitle);
		Gadget.log.info("  costcenter:" + costcenter);
		Gadget.log.info("  language:" + language);
		Gadget.log.info("  orgunit1globaluniqueid:" + orgunit1globaluniqueid);
		Gadget.log.info("  orgunit2globaluniqueid:" + orgunit2globaluniqueid);
		Gadget.log.info("  orgunit3globaluniqueid:" + orgunit3globaluniqueid);
		Gadget.log.info("  companyglobaluniqueid:" + companyglobaluniqueid);
		Gadget.log.info("  locationglobaluniqueid:" + locationglobaluniqueid);
		Gadget.log.info("  orgunit1name:" + orgunit1name);
		Gadget.log.info("  orgunit2name:" + orgunit2name);
		Gadget.log.info("  orgunit3name:" + orgunit3name);
		Gadget.log.info("  company:" + companyname);
		Gadget.log.info("  location:" + locationname);
		Gadget.log.info("  countrylong:" + countrylong);
		Gadget.log.info("  countrycodenumeric:" + countrycodenumeric);
		Gadget.log.info("  street:" + street);
		Gadget.log.info("  postalcode:" + postalcode);
		Gadget.log.info("  city:" + city);
		Gadget.log.info("  phone:" + phone);
		Gadget.log.info("  mobile:" + mobile);
		Gadget.log.info("  state:" + state);
		Gadget.log.info("  responsibleglobaluniqueid:" + responsibleglobaluniqueid);
		Gadget.log.info("  costresponsibleglobaluniqueid:" + costresponsibleglobaluniqueid);
		Gadget.log.info("  responsiblename:" + responsiblename);
		Gadget.log.info("  costresponsiblename:" + costresponsiblename);
		Gadget.log.info("  systemidsForUniqueAccountname:" + systemidsForUniqueAccountname);
		Gadget.log.info("  systemid:" + systemid);
		
		
		String actioninitiatorobjecttype="System";
		String actioninitiatoruniqueidpropertyname="systemid";
		String actioninitiatoruniqueid=getSystembyObject(database,"PersonCandidate","uniqueid",uniqueid);
		
		
		
		

		// Build workflow variables
		//CamundaVariable[] variables = buildEntryVariables(hmData);
		
		HashMap<String, String> startProcessVariables = new HashMap<String, String>();
		startProcessVariables.put("responsible", responsibleaccountname);
		startProcessVariables.put("systemidsForUniqueAccountname", systemidsForUniqueAccountname);
		startProcessVariables.put("systemid", systemid);
		startProcessVariables.put("givenname", givenname);
		startProcessVariables.put("surname", surname);
		startProcessVariables.put("gender", gender);
		startProcessVariables.put("orgunit1globaluniqueid", orgunit1globaluniqueid);
		startProcessVariables.put("orgunit2globaluniqueid", orgunit2globaluniqueid);
		startProcessVariables.put("orgunit3globaluniqueid", orgunit3globaluniqueid);
		startProcessVariables.put("companyglobaluniqueid", companyglobaluniqueid);
		startProcessVariables.put("locationglobaluniqueid", locationglobaluniqueid);
		startProcessVariables.put("orgunit1name", orgunit1name);
		startProcessVariables.put("orgunit2name", orgunit2name);
		startProcessVariables.put("orgunit3name", orgunit3name);
		startProcessVariables.put("company", companyname);
		startProcessVariables.put("location", locationname);
		startProcessVariables.put("language", language);
		startProcessVariables.put("salutation", salutation);
		startProcessVariables.put("title", title);
		startProcessVariables.put("jobtitle", jobtitle);
		startProcessVariables.put("costcenter", costcenter);
		startProcessVariables.put("countrylong", countrylong);
		startProcessVariables.put("countrycodenumeric", countrycodenumeric);
		startProcessVariables.put("street", street);
		startProcessVariables.put("postalcode", postalcode);
		startProcessVariables.put("city", city);
		startProcessVariables.put("state", state);
		startProcessVariables.put("phone", phone);
		startProcessVariables.put("mobile", mobile);
		startProcessVariables.put("responsibleglobaluniqueid", responsibleglobaluniqueid);
		startProcessVariables.put("costresponsibleglobaluniqueid", costresponsibleglobaluniqueid);
		startProcessVariables.put("responsiblename", responsiblename);
		startProcessVariables.put("costresponsiblename", costresponsiblename);
		startProcessVariables.put("uniqueid", uniqueid);
		startProcessVariables.put("actioninitiatorobjecttype", actioninitiatorobjecttype);
		startProcessVariables.put("actioninitiatoruniqueidpropertyname", actioninitiatoruniqueidpropertyname);
		startProcessVariables.put("actioninitiatoruniqueid", actioninitiatoruniqueid);
		startProcessVariables.put("system_requestsgadgetprotocol", Gadget.PROTOCOL);
		startProcessVariables.put("system_requestsgadgethost", Gadget.HOST);
		startProcessVariables.put("system_requestsgadgetport", Gadget.PORT);
		startProcessVariables.put("system_requestsgadgetpath", Gadget.PATH);
		startProcessVariables.put("system_requestsgadgetusername", "daccord");
		startProcessVariables.put("system_requestsgadgetpassword", "daccord");
		startProcessVariables.put("workflowengineid", "1");
		
		CamundaVariable[] variables = buildEntryVariables(startProcessVariables);
		

		Gadget.log.info("    Workflow Variables: {} variables prepared", variables.length);

		// Start workflow (commentable for testing!)
		//DaccordPersonLifecycleConsumerTask.startWorkflow("person-internal-add-auto", variables, workflowEngine);
		
		String camundaurl = workflowEngine.camundaurl();
		Gadget.log.info("CamundaURL:" + camundaurl);
		CamundaSystem camundasystem = new CamundaSystem(camundaurl);
		CamundaSession thiscamundasession;
		try {
			thiscamundasession = new CamundaSession(camundasystem, "admin", "admin");
			CamundaAction action = new CamundaAction(thiscamundasession);
			CamundaResponse gr = action.startProcess("person-internal-add-auto", variables);
			if (gr.wasSuccess()) {
				Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...handleEntry()..." + gr.getStatusmessage());
			} else {
				gr.getStatusmessage();
				Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...handleEntry()..." + gr.getStatusmessage());
				Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...handleEntry()..." + gr.getStatusXML());
			}
		} catch (CamundaException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private static CamundaVariable[] buildEntryVariables(HashMap<String, String> hmData) {

		List<CamundaVariable> variablesList = new ArrayList<>();

		// Add all PersonCandidate attributes as variables
		for (Map.Entry<String, String> entry : hmData.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();

			if (value != null && !value.isEmpty()) {
				variablesList.add(new CamundaVariable(key, value, CamundaVariable.type_STRING));
			}
		}

		return variablesList.toArray(new CamundaVariable[0]);
	}

	// ========================================
	// ACTION HANDLERS: CHANGE_OU
	// ========================================

	public static void handleChangeOU(HashMap<String, String> hmData, DACWorkflowEngine workflowEngine, Graph database) {

		// New values (already updated in PersonCandidate)
		String uniqueid = hmData.get("uniqueid");
		String givenname = hmData.get("givenname");
		String surname = hmData.get("surname");
		String newcompanyname = hmData.get("company");
		String newlocationname = hmData.get("location");
		String neworgunit1name = hmData.get("orgunit1name");
		String neworgunit2name = hmData.get("orgunit2name");
		String neworgunit3name = hmData.get("orgunit3name");

		// Old values + responsible from Person node
		String currentcompanyname = "";
		String currentlocationname = "";
		String currentorgunit1name = "";
		String currentorgunit2name = "";
		String currentorgunit3name = "";
		String givennamecurrentresponsible = "";
		String surnamecurrentresponsible = "";
		String fullnamecurrentresponsible = "";
		String currentresponsibleglobaluniqueid = "";
		String personglobaluniqueid = "";
		// New org globaluniqueid + uniqueid (for BPMN modifyRelation tasks)
		String newcompanyglobaluniqueid = "";
		String newcompanyuniqueid = "";
		String newlocationglobaluniqueid = "";
		String newlocationuniqueid = "";
		String neworgunit1globaluniqueid = "";
		String neworgunit1uniqueid = "";
		String neworgunit2globaluniqueid = "";
		String neworgunit2uniqueid = "";
		String neworgunit3globaluniqueid = "";
		String neworgunit3uniqueid = "";
		// Address data from new location
		String city = "";
		String street = "";
		String country = "";
		String countrycode = "";
		String postalcode = "";
		String countrycodenumeric = "";
		String state = "";
		String timezone = "";

		try {
			Map<String, Object> queryParameters = new HashMap();
			// Person globaluniqueid
			HashMap<String, String> person = database.getNodeAllPropertiesStringHashMap(
				"MATCH (:PersonCandidate {uniqueid:'" + uniqueid + "'})<-[:HAS_PERSONCANDIDATE]-(p:Person) RETURN p", queryParameters);
			personglobaluniqueid = person.get("globaluniqueid") != null ? person.get("globaluniqueid") : "";
			// Old company
			HashMap<String, String> company = database.getNodeAllPropertiesStringHashMap(
				"MATCH (:PersonCandidate {uniqueid:'" + uniqueid + "'})<-[:HAS_PERSONCANDIDATE]-(:Person)-[:EMPLOYED_BY]->(c:Company) RETURN c", queryParameters);
			currentcompanyname = company.get("name");
			// Old location
			HashMap<String, String> location = database.getNodeAllPropertiesStringHashMap(
				"MATCH (:PersonCandidate {uniqueid:'" + uniqueid + "'})<-[:HAS_PERSONCANDIDATE]-(:Person)-[:LOCATED_AT]->(l:Location) RETURN l", queryParameters);
			currentlocationname = location.get("name");
			// Old orgunit1
			HashMap<String, String> ou1 = database.getNodeAllPropertiesStringHashMap(
				"MATCH (:PersonCandidate {uniqueid:'" + uniqueid + "'})<-[:HAS_PERSONCANDIDATE]-(:Person)-[:ASSIGNED_TO_LEVEL1]->(o:Orgunit) RETURN o", queryParameters);
			currentorgunit1name = ou1.get("name");
			// Old orgunit2
			HashMap<String, String> ou2 = database.getNodeAllPropertiesStringHashMap(
				"MATCH (:PersonCandidate {uniqueid:'" + uniqueid + "'})<-[:HAS_PERSONCANDIDATE]-(:Person)-[:ASSIGNED_TO_LEVEL2]->(o:Orgunit) RETURN o", queryParameters);
			currentorgunit2name = ou2.get("name");
			// Old orgunit3
			HashMap<String, String> ou3 = database.getNodeAllPropertiesStringHashMap(
				"MATCH (:PersonCandidate {uniqueid:'" + uniqueid + "'})<-[:HAS_PERSONCANDIDATE]-(:Person)-[:ASSIGNED_TO_LEVEL3]->(o:Orgunit) RETURN o", queryParameters);
			currentorgunit3name = ou3.get("name");
			// Current responsible via IS_PERSONMANAGER
			HashMap<String, String> responsible = database.getNodeAllPropertiesStringHashMap(
				"MATCH (:PersonCandidate {uniqueid:'" + uniqueid + "'})<-[:HAS_PERSONCANDIDATE]-(:Person)<-[:IS_PERSONMANAGER]-(m:Person) RETURN m", queryParameters);
			givennamecurrentresponsible = responsible.get("givenname");
			surnamecurrentresponsible = responsible.get("surname");
			fullnamecurrentresponsible = givennamecurrentresponsible + " " + surnamecurrentresponsible;
			currentresponsibleglobaluniqueid = responsible.get("globaluniqueid") != null ? responsible.get("globaluniqueid") : "";
			// New company by code
			String companycode = hmData.get("companycode");
			if (companycode != null && !companycode.isEmpty()) {
				HashMap<String, String> newCompany = database.getNodeAllPropertiesStringHashMap("Company", "code", companycode);
				newcompanyglobaluniqueid = newCompany.get("globaluniqueid") != null ? newCompany.get("globaluniqueid") : "";
				newcompanyuniqueid = newCompany.get("uniqueid") != null ? newCompany.get("uniqueid") : "";
			}
			// New location by code
			String locationcode = hmData.get("locationcode");
			if (locationcode != null && !locationcode.isEmpty()) {
				HashMap<String, String> newLocation = database.getNodeAllPropertiesStringHashMap("Location", "code", locationcode);
				newlocationglobaluniqueid = newLocation.get("globaluniqueid") != null ? newLocation.get("globaluniqueid") : "";
				newlocationuniqueid = newLocation.get("uniqueid") != null ? newLocation.get("uniqueid") : "";
				city = newLocation.get("city") != null ? newLocation.get("city") : "";
				street = newLocation.get("street") != null ? newLocation.get("street") : "";
				country = newLocation.get("country") != null ? newLocation.get("country") : "";
				countrycode = newLocation.get("countrycode") != null ? newLocation.get("countrycode") : "";
				postalcode = newLocation.get("postalcode") != null ? newLocation.get("postalcode") : "";
				countrycodenumeric = newLocation.get("countrycodenumeric") != null ? newLocation.get("countrycodenumeric") : "";
				state = newLocation.get("state") != null ? newLocation.get("state") : "";
				timezone = newLocation.get("timezone") != null ? newLocation.get("timezone") : "";
			}
			// New orgunit1 by code
			String orgunit1code = hmData.get("orgunit1code");
			if (orgunit1code != null && !orgunit1code.isEmpty()) {
				HashMap<String, String> newOu1 = database.getNodeAllPropertiesStringHashMap("Orgunit", "code", orgunit1code);
				neworgunit1globaluniqueid = newOu1.get("globaluniqueid") != null ? newOu1.get("globaluniqueid") : "";
				neworgunit1uniqueid = newOu1.get("uniqueid") != null ? newOu1.get("uniqueid") : "";
			}
			// New orgunit2 by code
			String orgunit2code = hmData.get("orgunit2code");
			if (orgunit2code != null && !orgunit2code.isEmpty()) {
				HashMap<String, String> newOu2 = database.getNodeAllPropertiesStringHashMap("Orgunit", "code", orgunit2code);
				neworgunit2globaluniqueid = newOu2.get("globaluniqueid") != null ? newOu2.get("globaluniqueid") : "";
				neworgunit2uniqueid = newOu2.get("uniqueid") != null ? newOu2.get("uniqueid") : "";
			}
			// New orgunit3 by code
			String orgunit3code = hmData.get("orgunit3code");
			if (orgunit3code != null && !orgunit3code.isEmpty()) {
				HashMap<String, String> newOu3 = database.getNodeAllPropertiesStringHashMap("Orgunit", "code", orgunit3code);
				neworgunit3globaluniqueid = newOu3.get("globaluniqueid") != null ? newOu3.get("globaluniqueid") : "";
				neworgunit3uniqueid = newOu3.get("uniqueid") != null ? newOu3.get("uniqueid") : "";
			}
		} catch (GraphException e) {
			e.printStackTrace();
		}

		String actioninitiatorobjecttype = "System";
		String actioninitiatoruniqueidpropertyname = "systemid";
		String actioninitiatoruniqueid = getSystembyObject(database, "PersonCandidate", "uniqueid", uniqueid);

		Gadget.log.info("  CHANGE_OU Handler:");
		Gadget.log.info("    Unique ID: {}", uniqueid);
		Gadget.log.info("    Name: {} {}", givenname, surname);
		Gadget.log.info("    Old Org: {} / {} / {} / {}", currentcompanyname, currentlocationname, currentorgunit1name, currentorgunit2name);
		Gadget.log.info("    New Org: {} / {} / {} / {}", newcompanyname, newlocationname, neworgunit1name, neworgunit2name);
		Gadget.log.info("    Responsible: {} {}", givennamecurrentresponsible, surnamecurrentresponsible);

		// Build workflow variables
		HashMap<String, String> startProcessVariables = new HashMap<String, String>();
		startProcessVariables.put("uniqueid", uniqueid);
		startProcessVariables.put("givenname", givenname);
		startProcessVariables.put("surname", surname);
		startProcessVariables.put("currentcompanyname", currentcompanyname);
		startProcessVariables.put("currentlocationname", currentlocationname);
		startProcessVariables.put("currentorgunit1name", currentorgunit1name);
		startProcessVariables.put("currentorgunit2name", currentorgunit2name);
		startProcessVariables.put("currentorgunit3name", currentorgunit3name);
		startProcessVariables.put("newcompanyname", newcompanyname);
		startProcessVariables.put("newlocationname", newlocationname);
		startProcessVariables.put("neworgunit1name", neworgunit1name);
		startProcessVariables.put("neworgunit2name", neworgunit2name);
		startProcessVariables.put("neworgunit3name", neworgunit3name);
		startProcessVariables.put("givennamecurrentresponsible", givennamecurrentresponsible);
		startProcessVariables.put("surnamecurrentresponsible", surnamecurrentresponsible);
		startProcessVariables.put("fullnamecurrentresponsible", fullnamecurrentresponsible);
		startProcessVariables.put("currentresponsibleglobaluniqueid", currentresponsibleglobaluniqueid);
		startProcessVariables.put("newcompanyglobaluniqueid", newcompanyglobaluniqueid);
		startProcessVariables.put("newcompanyuniqueid", newcompanyuniqueid);
		startProcessVariables.put("newlocationglobaluniqueid", newlocationglobaluniqueid);
		startProcessVariables.put("newlocationuniqueid", newlocationuniqueid);
		startProcessVariables.put("neworgunit1globaluniqueid", neworgunit1globaluniqueid);
		startProcessVariables.put("neworgunit1uniqueid", neworgunit1uniqueid);
		startProcessVariables.put("neworgunit2globaluniqueid", neworgunit2globaluniqueid);
		startProcessVariables.put("neworgunit2uniqueid", neworgunit2uniqueid);
		startProcessVariables.put("neworgunit3globaluniqueid", neworgunit3globaluniqueid);
		startProcessVariables.put("neworgunit3uniqueid", neworgunit3uniqueid);
		startProcessVariables.put("city", city);
		startProcessVariables.put("street", street);
		startProcessVariables.put("country", country);
		startProcessVariables.put("countrycode", countrycode);
		startProcessVariables.put("postalcode", postalcode);
		startProcessVariables.put("countrycodenumeric", countrycodenumeric);
		startProcessVariables.put("state", state);
		startProcessVariables.put("timezone", timezone);
		startProcessVariables.put("displaytab2", "false");
		startProcessVariables.put("actioninitiatorobjecttype", actioninitiatorobjecttype);
		startProcessVariables.put("actioninitiatoruniqueidpropertyname", actioninitiatoruniqueidpropertyname);
		startProcessVariables.put("actioninitiatoruniqueid", actioninitiatoruniqueid);
		startProcessVariables.put("processinitiatorobjecttype", actioninitiatorobjecttype);
		startProcessVariables.put("processinitiatoruniqueidpropertyname", actioninitiatoruniqueidpropertyname);
		startProcessVariables.put("processinitiatoruniqueid", actioninitiatoruniqueid);
		startProcessVariables.put("processtargetobjecttype", "Person");
		startProcessVariables.put("processtargetuniqueidpropertyname", "globaluniqueid");
		startProcessVariables.put("processtargetuniqueid", personglobaluniqueid);
		startProcessVariables.put("system_requestsgadgetprotocol", Gadget.PROTOCOL);
		startProcessVariables.put("system_requestsgadgethost", Gadget.HOST);
		startProcessVariables.put("system_requestsgadgetport", Gadget.PORT);
		startProcessVariables.put("system_requestsgadgetpath", Gadget.PATH);
		startProcessVariables.put("system_requestsgadgetusername", "daccord");
		startProcessVariables.put("system_requestsgadgetpassword", "daccord");
		startProcessVariables.put("workflowengineid", "1");

		CamundaVariable[] variables = buildChangeOUVariables(startProcessVariables);

		Gadget.log.info("    Workflow Variables: {} variables prepared", variables.length);

		// Start workflow
		String camundaurl = workflowEngine.camundaurl();
		Gadget.log.info("CamundaURL:" + camundaurl);
		CamundaSystem camundasystem = new CamundaSystem(camundaurl);
		CamundaSession thiscamundasession;
		try {
			thiscamundasession = new CamundaSession(camundasystem, "admin", "admin");
			CamundaAction action = new CamundaAction(thiscamundasession);
			CamundaResponse gr = action.startProcess("person-mod-org", variables);
			if (gr.wasSuccess()) {
				Gadget.log.debug("PersonCandidateTaskHandler...handleChangeOU()..." + gr.getStatusmessage());
			} else {
				Gadget.log.debug("PersonCandidateTaskHandler...handleChangeOU()..." + gr.getStatusmessage());
				Gadget.log.debug("PersonCandidateTaskHandler...handleChangeOU()..." + gr.getStatusXML());
			}
		} catch (CamundaException e) {
			e.printStackTrace();
		}
	}

	private static CamundaVariable[] buildChangeOUVariables(HashMap<String, String> hmData) {

		List<CamundaVariable> variablesList = new ArrayList<>();

		for (Map.Entry<String, String> entry : hmData.entrySet()) {
			variablesList.add(new CamundaVariable(entry.getKey(), entry.getValue(), CamundaVariable.type_STRING));
		}

		return variablesList.toArray(new CamundaVariable[0]);
	}

	// ========================================
	// ACTION HANDLERS: CHANGE_NAME
	// ========================================

	public static void handleChangeName(HashMap<String, String> hmData, DACWorkflowEngine workflowEngine, Graph database) {

		// New values (already updated in PersonCandidate)
		String uniqueid = hmData.get("uniqueid");
		String givenname = hmData.get("givenname");
		String surname = hmData.get("surname");
		String gender = hmData.get("gender");
		String salutation = "";
		if ("M".equals(gender)) salutation = "Herr";
		else if ("F".equals(gender)) salutation = "Frau";

		// Old values + responsible from Person node
		String currentgivenname = "";
		String currentsurname = "";
		String currentgender = "";
		String currentsalutation = "";
		String givennamecurrentresponsible = "";
		String surnamecurrentresponsible = "";
		String currentresponsibleperson = "";
		String currentresponsibleglobaluniqueid = "";
		String personglobaluniqueid = "";

		try {
			Map<String, Object> queryParameters = new HashMap();
			// Old name values from Person node (not yet updated)
			HashMap<String, String> person = database.getNodeAllPropertiesStringHashMap(
				"MATCH (:PersonCandidate {uniqueid:'" + uniqueid + "'})<-[:HAS_PERSONCANDIDATE]-(p:Person) RETURN p", queryParameters);
			currentgivenname = person.get("givenname");
			currentsurname = person.get("surname");
			currentgender = person.get("gender");
			currentsalutation = person.get("salutation");
			personglobaluniqueid = person.get("globaluniqueid") != null ? person.get("globaluniqueid") : "";
			if (currentsalutation == null || currentsalutation.isEmpty()) {
				if ("M".equals(currentgender)) currentsalutation = "Herr";
				else if ("F".equals(currentgender)) currentsalutation = "Frau";
			}
			// Current responsible via IS_PERSONMANAGER
			HashMap<String, String> responsible = database.getNodeAllPropertiesStringHashMap(
				"MATCH (:PersonCandidate {uniqueid:'" + uniqueid + "'})<-[:HAS_PERSONCANDIDATE]-(:Person)<-[:IS_PERSONMANAGER]-(m:Person) RETURN m", queryParameters);
			givennamecurrentresponsible = responsible.get("givenname");
			surnamecurrentresponsible = responsible.get("surname");
			currentresponsibleperson = getAccountById(database, responsible.get("uniqueid"));
			currentresponsibleglobaluniqueid = responsible.get("globaluniqueid") != null ? responsible.get("globaluniqueid") : "";
		} catch (GraphException e) {
			e.printStackTrace();
		}

		String actioninitiatorobjecttype = "System";
		String actioninitiatoruniqueidpropertyname = "systemid";
		String actioninitiatoruniqueid = getSystembyObject(database, "PersonCandidate", "uniqueid", uniqueid);

		Gadget.log.info("  CHANGE_NAME Handler:");
		Gadget.log.info("    Unique ID: {}", uniqueid);
		Gadget.log.info("    New Name: {} {} (gender: {}, salutation: {})", givenname, surname, gender, salutation);
		Gadget.log.info("    Old Name: {} {} (gender: {}, salutation: {})", currentgivenname, currentsurname, currentgender, currentsalutation);
		Gadget.log.info("    Responsible: {} {}", givennamecurrentresponsible, surnamecurrentresponsible);

		// Build workflow variables
		HashMap<String, String> startProcessVariables = new HashMap<String, String>();
		startProcessVariables.put("uniqueid", uniqueid);
		startProcessVariables.put("givenname", givenname);
		startProcessVariables.put("surname", surname);
		startProcessVariables.put("gender", gender);
		startProcessVariables.put("salutation", salutation);
		startProcessVariables.put("currentgivenname", currentgivenname);
		startProcessVariables.put("currentsurname", currentsurname);
		startProcessVariables.put("currentgender", currentgender);
		startProcessVariables.put("currentsalutation", currentsalutation);
		startProcessVariables.put("givennamecurrentresponsible", givennamecurrentresponsible);
		startProcessVariables.put("surnamecurrentresponsible", surnamecurrentresponsible);
		startProcessVariables.put("currentresponsibleperson", currentresponsibleperson);
		startProcessVariables.put("newresponsibleperson", currentresponsibleperson);
		startProcessVariables.put("currentresponsibleglobaluniqueid", currentresponsibleglobaluniqueid);
		startProcessVariables.put("processinitiatorpersonglobauniqueid", "");
		startProcessVariables.put("actioninitiatorobjecttype", actioninitiatorobjecttype);
		startProcessVariables.put("actioninitiatoruniqueidpropertyname", actioninitiatoruniqueidpropertyname);
		startProcessVariables.put("actioninitiatoruniqueid", actioninitiatoruniqueid);
		startProcessVariables.put("processinitiatorobjecttype", actioninitiatorobjecttype);
		startProcessVariables.put("processinitiatoruniqueidpropertyname", actioninitiatoruniqueidpropertyname);
		startProcessVariables.put("processinitiatoruniqueid", actioninitiatoruniqueid);
		startProcessVariables.put("processtargetobjecttype", "Person");
		startProcessVariables.put("processtargetuniqueidpropertyname", "globaluniqueid");
		startProcessVariables.put("processtargetuniqueid", personglobaluniqueid);
		startProcessVariables.put("system_requestsgadgetprotocol", Gadget.PROTOCOL);
		startProcessVariables.put("system_requestsgadgethost", Gadget.HOST);
		startProcessVariables.put("system_requestsgadgetport", Gadget.PORT);
		startProcessVariables.put("system_requestsgadgetpath", Gadget.PATH);
		startProcessVariables.put("system_requestsgadgetusername", "daccord");
		startProcessVariables.put("system_requestsgadgetpassword", "daccord");
		startProcessVariables.put("workflowengineid", "1");

		CamundaVariable[] variables = buildChangeNameVariables(startProcessVariables);

		Gadget.log.info("    Workflow Variables: {} variables prepared", variables.length);

		// Start workflow
		String camundaurl = workflowEngine.camundaurl();
		Gadget.log.info("CamundaURL:" + camundaurl);
		CamundaSystem camundasystem = new CamundaSystem(camundaurl);
		CamundaSession thiscamundasession;
		try {
			thiscamundasession = new CamundaSession(camundasystem, "admin", "admin");
			CamundaAction action = new CamundaAction(thiscamundasession);
			CamundaResponse gr = action.startProcess("person-mod-name", variables);
			if (gr.wasSuccess()) {
				Gadget.log.debug("PersonCandidateTaskHandler...handleChangeName()..." + gr.getStatusmessage());
			} else {
				Gadget.log.debug("PersonCandidateTaskHandler...handleChangeName()..." + gr.getStatusmessage());
				Gadget.log.debug("PersonCandidateTaskHandler...handleChangeName()..." + gr.getStatusXML());
			}
		} catch (CamundaException e) {
			e.printStackTrace();
		}
	}

	private static CamundaVariable[] buildChangeNameVariables(HashMap<String, String> hmData) {

		List<CamundaVariable> variablesList = new ArrayList<>();

		for (Map.Entry<String, String> entry : hmData.entrySet()) {
			variablesList.add(new CamundaVariable(entry.getKey(), entry.getValue(), CamundaVariable.type_STRING));
		}

		return variablesList.toArray(new CamundaVariable[0]);
	}

	// ========================================
	// ACTION HANDLERS: CHANGE_RESP
	// ========================================

	public static void handleChangeResp(HashMap<String, String> hmData, DACWorkflowEngine workflowEngine, Graph database) {

		String uniqueid = hmData.get("uniqueid");
		String givenname = hmData.get("givenname");
		String surname = hmData.get("surname");
		String manageruniqueid = hmData.get("manageruniqueid");
		String currentresponsibleglobaluniqueid="";
		String newresponsibleglobaluniqueid="";
		String responsibleaccountname ="";
		String currentresponsiblegivenname="";
		String currentresponsiblesurname="";
		String givennamenewresponsible="";
		String surnamenewresponsible="";
		String fullnamenewresponsible ="";
		String personglobaluniqueid = "";

		//Person zu PersonCandidate
		try {
			HashMap<String, String> person = database.getNodeAllPropertiesStringHashMap("Person", "employeeid", uniqueid);
			personglobaluniqueid = person.get("globaluniqueid") != null ? person.get("globaluniqueid") : "";
			Map<String, Object> queryParameters = new HashMap();
			HashMap<String, String> currentresponsble = database.getNodeAllPropertiesStringHashMap("MATCH (:Person {employeeid:'"+uniqueid+"'})<-[:IS_PERSONMANAGER]-(p:Person) return p", queryParameters);
			currentresponsibleglobaluniqueid = currentresponsble.get("globaluniqueid");
			currentresponsiblegivenname = currentresponsble.get("givenname");
			currentresponsiblesurname = currentresponsble.get("surname");
			HashMap<String, String> newresponsible = database.getNodeAllPropertiesStringHashMap("Person", "uniqueid", manageruniqueid);
			
			newresponsibleglobaluniqueid = newresponsible.get("globaluniqueid");
			givennamenewresponsible = newresponsible.get("givenname");
			surnamenewresponsible = newresponsible.get("surname");
			fullnamenewresponsible =givennamenewresponsible +" "+ surnamenewresponsible;
			responsibleaccountname = getAccountById(database, manageruniqueid);

			
		} catch (GraphException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Gadget.log.info("  CHANGE_RESP Handler:");
		Gadget.log.info("    Unique ID: {}", uniqueid);
		Gadget.log.info("    Name: {} {}", givenname, surname);
		Gadget.log.info("    New Manager ID: {}", manageruniqueid);
		Gadget.log.info("    New Manager Global ID: {}", newresponsibleglobaluniqueid);
		Gadget.log.info("    Old Manager ID: {}", currentresponsibleglobaluniqueid);



		String actioninitiatorobjecttype="System";
		String actioninitiatoruniqueidpropertyname="systemid";
		String actioninitiatoruniqueid=getSystembyObject(database,"PersonCandidate","uniqueid",uniqueid);
		// Build workflow variables
		HashMap<String, String> startProcessVariables = new HashMap<String, String>();
		startProcessVariables.put("newresponsibleaccountname", responsibleaccountname);
		startProcessVariables.put("newcostresponsibleaccountname", responsibleaccountname);
		startProcessVariables.put("currentresponsibleglobaluniqueid", currentresponsibleglobaluniqueid);
		startProcessVariables.put("newresponsibleglobaluniqueid", newresponsibleglobaluniqueid);
		startProcessVariables.put("newresponsibleaccountglobaluniqueid", "1234");//Ungleich initiator
		startProcessVariables.put("givennamecurrentresponsible", currentresponsiblegivenname);
		startProcessVariables.put("surnamecurrentresponsible", currentresponsiblesurname);
		startProcessVariables.put("fullnamenewresponsible", fullnamenewresponsible);
		startProcessVariables.put("fullnamecurrentresponsible", currentresponsiblegivenname+" "+currentresponsiblesurname);
		startProcessVariables.put("currentcostresponsibleglobaluniqueid", currentresponsibleglobaluniqueid);
		startProcessVariables.put("newcostresponsibleglobaluniqueid", newresponsibleglobaluniqueid);
		startProcessVariables.put("newcostresponsibleaccountglobaluniqueid", "1234");//Ungleich initiator
		startProcessVariables.put("givennamecurrentcostresponsible", currentresponsiblegivenname);
		startProcessVariables.put("surnamecurrentcostresponsible", currentresponsiblesurname);
		startProcessVariables.put("fullnamenewcostresponsible", fullnamenewresponsible);
		startProcessVariables.put("fullnamecurrentcostresponsible", currentresponsiblegivenname+" "+currentresponsiblesurname);
		startProcessVariables.put("givenname", givenname);
		startProcessVariables.put("surname", surname);
		startProcessVariables.put("uniqueid", uniqueid);
		startProcessVariables.put("actioninitiatorobjecttype", actioninitiatorobjecttype);
		startProcessVariables.put("actioninitiatoruniqueidpropertyname", actioninitiatoruniqueidpropertyname);
		startProcessVariables.put("actioninitiatoruniqueid", actioninitiatoruniqueid);
		startProcessVariables.put("processinitiatorobjecttype", actioninitiatorobjecttype);
		startProcessVariables.put("processinitiatoruniqueidpropertyname", actioninitiatoruniqueidpropertyname);
		startProcessVariables.put("processinitiatoruniqueid", actioninitiatoruniqueid);
		startProcessVariables.put("processtargetobjecttype", "Person");
		startProcessVariables.put("processtargetuniqueidpropertyname", "globaluniqueid");
		startProcessVariables.put("processtargetuniqueid", personglobaluniqueid);
		startProcessVariables.put("system_requestsgadgetprotocol", Gadget.PROTOCOL);
		startProcessVariables.put("system_requestsgadgethost", Gadget.HOST);
		startProcessVariables.put("system_requestsgadgetport", Gadget.PORT);
		startProcessVariables.put("system_requestsgadgetpath", Gadget.PATH);
		startProcessVariables.put("system_requestsgadgetusername", "daccord");
		startProcessVariables.put("system_requestsgadgetpassword", "daccord");
		startProcessVariables.put("workflowengineid", "1");
		
		CamundaVariable[] variables = buildChangeRespVariables(startProcessVariables);

		Gadget.log.info("    Workflow Variables: {} variables prepared", variables.length);
		
		
		// Start workflow (commentable for testing!)
		String camundaurl = workflowEngine.camundaurl();
		Gadget.log.info("CamundaURL:" + camundaurl);
		CamundaSystem camundasystem = new CamundaSystem(camundaurl);
		CamundaSession thiscamundasession;
		try {
			thiscamundasession = new CamundaSession(camundasystem, "admin", "admin");
			CamundaAction action = new CamundaAction(thiscamundasession);
			CamundaResponse gr = action.startProcess("person-mod-resp", variables);
			CamundaResponse gr2 = action.startProcess("person-mod-costresp", variables);
			if (gr2.wasSuccess() && gr.wasSuccess()) {
				Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...changeResp()..." + gr.getStatusmessage());
				Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...changeCostResp()..." + gr2.getStatusmessage());
			} else {
				gr2.getStatusmessage();
				Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...changeCostResp()..." + gr2.getStatusmessage());
				Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...changeCostResp()..." + gr2.getStatusXML());
				gr.getStatusmessage();
				Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...changeResp()..." + gr.getStatusmessage());
				Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...changeResp()..." + gr.getStatusXML());
			}
		} catch (CamundaException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		

	}

	private static CamundaVariable[] buildChangeRespVariables(HashMap<String, String> hmData) {

		List<CamundaVariable> variablesList = new ArrayList<>();

		// Add all PersonCandidate attributes as variables
		for (Map.Entry<String, String> entry : hmData.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();

			if (value != null && !value.isEmpty()) {
				variablesList.add(new CamundaVariable(key, value, CamundaVariable.type_STRING));
			}
		}

		return variablesList.toArray(new CamundaVariable[0]);
	}

	// ========================================
	// ACTION HANDLERS: CHANGE_ABSENCE
	// ========================================

	public static void handleChangeAbsence(HashMap<String, String> hmData, DACWorkflowEngine workflowEngine, Graph database) {

		// Values from PersonCandidate
		String uniqueid           = hmData.get("uniqueid");
		String givenname          = hmData.get("givenname");
		String surname            = hmData.get("surname");
		String dateofabsencestart = (hmData.get("dateofabsencestart") != null && !hmData.get("dateofabsencestart").isEmpty()) ? hmData.get("dateofabsencestart") : "REMOVE";
		String dateofabsenceend   = (hmData.get("dateofabsenceend")   != null && !hmData.get("dateofabsenceend").isEmpty())   ? hmData.get("dateofabsenceend")   : "REMOVE";
		String statusreason       = (hmData.get("statusreason")       != null && !hmData.get("statusreason").isEmpty())       ? hmData.get("statusreason")       : "REMOVE";

		// Person globaluniqueid + responsibleglobaluniqueid from DB
		String personglobaluniqueid      = "";
		String responsibleglobaluniqueid = "";
		try {
			HashMap<String, String> person = database.getNodeAllPropertiesStringHashMap("Person", "employeeid", uniqueid);
			personglobaluniqueid = person.get("globaluniqueid") != null ? person.get("globaluniqueid") : "";
			// Responsible: lookup via IS_PERSONMANAGER (manager points TO person)
			if (!personglobaluniqueid.isEmpty()) {
				HashMap<String, Object> mgParams = new HashMap<>();
				mgParams.put("guid", personglobaluniqueid);
				List<Record> mgRecords = database.execute(
					"MATCH (m:Person)-[:IS_PERSONMANAGER]->(p:Person {globaluniqueid: $guid}) RETURN m.globaluniqueid AS manager_guid LIMIT 1",
					mgParams);
				if (!mgRecords.isEmpty()) {
					responsibleglobaluniqueid = mgRecords.get(0).get("manager_guid").asString();
				}
			}
		} catch (GraphException e) {
			e.printStackTrace();
		}

		String actioninitiatorobjecttype           = "System";
		String actioninitiatoruniqueidpropertyname = "systemid";
		String actioninitiatoruniqueid             = getSystembyObject(database, "PersonCandidate", "uniqueid", uniqueid);

		Gadget.log.info("  CHANGE_ABSENCE Handler:");
		Gadget.log.info("    Unique ID: {}", uniqueid);
		Gadget.log.info("    Name: {} {}", givenname, surname);
		Gadget.log.info("    Absence Start: {}", dateofabsencestart);
		Gadget.log.info("    Absence End: {}", dateofabsenceend);
		Gadget.log.info("    Status reason: {}", statusreason);
		Gadget.log.info("    Responsible (globaluniqueid): {}", responsibleglobaluniqueid);

		// Build workflow variables
		HashMap<String, String> startProcessVariables = new HashMap<String, String>();
		startProcessVariables.put("uniqueid",                             uniqueid);
		startProcessVariables.put("givenname",                           givenname);
		startProcessVariables.put("surname",                             surname);
		startProcessVariables.put("dateofabsencestart",                  dateofabsencestart);
		startProcessVariables.put("dateofabsenceend",                    dateofabsenceend);
		startProcessVariables.put("statusreason",                        statusreason);
		startProcessVariables.put("responsibleglobaluniqueid",           responsibleglobaluniqueid);
		startProcessVariables.put("actioninitiatorobjecttype",           actioninitiatorobjecttype);
		startProcessVariables.put("actioninitiatoruniqueidpropertyname", actioninitiatoruniqueidpropertyname);
		startProcessVariables.put("actioninitiatoruniqueid",             actioninitiatoruniqueid);
		startProcessVariables.put("processinitiatorobjecttype",          actioninitiatorobjecttype);
		startProcessVariables.put("processinitiatoruniqueidpropertyname", actioninitiatoruniqueidpropertyname);
		startProcessVariables.put("processinitiatoruniqueid",            actioninitiatoruniqueid);
		startProcessVariables.put("processtargetobjecttype",             "Person");
		startProcessVariables.put("processtargetuniqueidpropertyname",   "globaluniqueid");
		startProcessVariables.put("processtargetuniqueid",               personglobaluniqueid);
		startProcessVariables.put("system_requestsgadgetprotocol",       Gadget.PROTOCOL);
		startProcessVariables.put("system_requestsgadgethost",           Gadget.HOST);
		startProcessVariables.put("system_requestsgadgetport",           Gadget.PORT);
		startProcessVariables.put("system_requestsgadgetpath",           Gadget.PATH);
		startProcessVariables.put("system_requestsgadgetusername",       "daccord");
		startProcessVariables.put("system_requestsgadgetpassword",       "daccord");
		startProcessVariables.put("workflowengineid",                    "1");

		List<CamundaVariable> variablesList = new ArrayList<>();
		for (Map.Entry<String, String> entry : startProcessVariables.entrySet()) {
			variablesList.add(new CamundaVariable(entry.getKey(), entry.getValue(), CamundaVariable.type_STRING));
		}
		CamundaVariable[] variables = variablesList.toArray(new CamundaVariable[0]);

		Gadget.log.info("    Workflow Variables: {} variables prepared", variables.length);

		// Start workflow
		String camundaurl = workflowEngine.camundaurl();
		Gadget.log.info("CamundaURL:" + camundaurl);
		CamundaSystem camundasystem = new CamundaSystem(camundaurl);
		try {
			CamundaSession thiscamundasession = new CamundaSession(camundasystem, "admin", "admin");
			CamundaAction action = new CamundaAction(thiscamundasession);
			CamundaResponse gr = action.startProcess("person-mod-absence-auto", variables);
			if (gr.wasSuccess()) {
				Gadget.log.debug("PersonCandidateTaskHandler...handleChangeAbsence()..." + gr.getStatusmessage());
			} else {
				Gadget.log.debug("PersonCandidateTaskHandler...handleChangeAbsence()..." + gr.getStatusmessage());
				Gadget.log.debug("PersonCandidateTaskHandler...handleChangeAbsence()..." + gr.getStatusXML());
			}
		} catch (CamundaException e) {
			e.printStackTrace();
		}
	}

	// ========================================
	// ACTION HANDLERS: MODIFY
	// ========================================

	public static void handleModify(HashMap<String, String> hmData, DACWorkflowEngine workflowEngine, Graph database) {

		// Values from PersonCandidate
		String uniqueid          = hmData.get("uniqueid");
		String givenname         = hmData.get("givenname");
		String surname           = hmData.get("surname");
		String phone  = (hmData.get("phone") != null && !hmData.get("phone").isEmpty()) ? hmData.get("phone") : "REMOVE";
		String mobile            = (hmData.get("mobile")          != null && !hmData.get("mobile").isEmpty())          ? hmData.get("mobile")          : "REMOVE";
		String costcenter        = (hmData.get("costcenter")      != null && !hmData.get("costcenter").isEmpty())      ? hmData.get("costcenter")      : "REMOVE";
		String employeeid       = 	hmData.get("uniqueid");
		String position          = (hmData.get("position")        != null && !hmData.get("position").isEmpty())        ? hmData.get("position")        : "REMOVE";
		String jobtitle          = (hmData.get("jobtitle")        != null && !hmData.get("jobtitle").isEmpty())        ? hmData.get("jobtitle")        : "REMOVE";
		String leavedate         = (hmData.get("leavedate")       != null && !hmData.get("leavedate").isEmpty())       ? hmData.get("leavedate")       : "REMOVE";

		// Person globaluniqueid from DB
		String personglobaluniqueid = "";
		try {
			HashMap<String, String> person = database.getNodeAllPropertiesStringHashMap("Person", "employeeid", uniqueid);
			personglobaluniqueid = person.get("globaluniqueid") != null ? person.get("globaluniqueid") : "";
		} catch (GraphException e) {
			e.printStackTrace();
		}

		String actioninitiatorobjecttype          = "System";
		String actioninitiatoruniqueidpropertyname = "systemid";
		String actioninitiatoruniqueid             = getSystembyObject(database, "PersonCandidate", "uniqueid", uniqueid);

		Gadget.log.info("  MODIFY Handler:");
		Gadget.log.info("    Unique ID: {}", uniqueid);
		Gadget.log.info("    Name: {} {}", givenname, surname);
		Gadget.log.info("    phone: {}, mobile: {}, costcenter: {}", phone, mobile, costcenter);
		Gadget.log.info("    workforceid: {}, position: {}, jobtitle: {}", employeeid, position, jobtitle);
		Gadget.log.info("    leavedate: {}", leavedate);

		// Build workflow variables
		HashMap<String, String> startProcessVariables = new HashMap<String, String>();
		startProcessVariables.put("uniqueid",           uniqueid);
		startProcessVariables.put("givenname",          givenname);
		startProcessVariables.put("surname",            surname);
		startProcessVariables.put("phone",    phone);
		startProcessVariables.put("mobile",             mobile);
		startProcessVariables.put("costcenter",         costcenter);
		startProcessVariables.put("employeeid",        employeeid);
		startProcessVariables.put("position",           position);
		startProcessVariables.put("jobtitle",           jobtitle);
		startProcessVariables.put("leavedate",          leavedate);
		startProcessVariables.put("actioninitiatorobjecttype",           actioninitiatorobjecttype);
		startProcessVariables.put("actioninitiatoruniqueidpropertyname", actioninitiatoruniqueidpropertyname);
		startProcessVariables.put("actioninitiatoruniqueid",             actioninitiatoruniqueid);
		startProcessVariables.put("processinitiatorobjecttype",          actioninitiatorobjecttype);
		startProcessVariables.put("processinitiatoruniqueidpropertyname", actioninitiatoruniqueidpropertyname);
		startProcessVariables.put("processinitiatoruniqueid",            actioninitiatoruniqueid);
		startProcessVariables.put("processtargetobjecttype",             "Person");
		startProcessVariables.put("processtargetuniqueidpropertyname",   "globaluniqueid");
		startProcessVariables.put("processtargetuniqueid",               personglobaluniqueid);
		startProcessVariables.put("system_requestsgadgetprotocol",       Gadget.PROTOCOL);
		startProcessVariables.put("system_requestsgadgethost",           Gadget.HOST);
		startProcessVariables.put("system_requestsgadgetport",           Gadget.PORT);
		startProcessVariables.put("system_requestsgadgetpath",           Gadget.PATH);
		startProcessVariables.put("system_requestsgadgetusername",       "daccord");
		startProcessVariables.put("system_requestsgadgetpassword",       "daccord");
		startProcessVariables.put("workflowengineid",                    "1");

		List<CamundaVariable> variablesList = new ArrayList<>();
		for (Map.Entry<String, String> entry : startProcessVariables.entrySet()) {
			variablesList.add(new CamundaVariable(entry.getKey(), entry.getValue(), CamundaVariable.type_STRING));
		}
		CamundaVariable[] variables = variablesList.toArray(new CamundaVariable[0]);

		Gadget.log.info("    Workflow Variables: {} variables prepared", variables.length);

		// Start workflow
		String camundaurl = workflowEngine.camundaurl();
		Gadget.log.info("CamundaURL:" + camundaurl);
		CamundaSystem camundasystem = new CamundaSystem(camundaurl);
		try {
			CamundaSession thiscamundasession = new CamundaSession(camundasystem, "admin", "admin");
			CamundaAction action = new CamundaAction(thiscamundasession);
			CamundaResponse gr = action.startProcess("person-mod-attributes-auto", variables);
			if (gr.wasSuccess()) {
				Gadget.log.debug("PersonCandidateTaskHandler...handleModify()..." + gr.getStatusmessage());
			} else {
				Gadget.log.debug("PersonCandidateTaskHandler...handleModify()..." + gr.getStatusmessage());
				Gadget.log.debug("PersonCandidateTaskHandler...handleModify()..." + gr.getStatusXML());
			}
		} catch (CamundaException e) {
			e.printStackTrace();
		}
	}
	
	/*
	public DACWorkflowEngine getWorkflowEngine(DACSession dacsession, String workflowengineid) {

		HashMap<String, DACWorkflowEngine> hmWorkflowEngines = (HashMap<String, DACWorkflowEngine>) this.getData("hmWorkflowEngines");
		DACWorkflowEngine engine = hmWorkflowEngines.get(workflowengineid);
		if (engine == null) {
			engine = new DACWorkflowEngine(dacsession, workflowengineid);
			if (engine.isReady())
				hmWorkflowEngines.put(workflowengineid, engine);
		}
		return engine;
	}*/
	
	//==========================================
	//Hilfsmehtoden
	//==========================================
	private static String getAccountById(Graph database, String responsibleid) {
		String name = "";
		try {
			String statement = "MATCH (p:Person {uniqueid:'" + responsibleid +
				"'})-[:HAS_SECURITYPRINCIPAL]->(a:Account)<-[:OWNS_ACCOUNT]-(:System)<-[:IS_AUTHORIZATIONSYSTEM]-(:Daccord)  RETURN a.name AS name;";
			Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...getAccountById...statement:" + statement);
			XML xmlManager = database.getXML(statement, "Accounts", "account");
			Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...getAccountById...toString:" + xmlManager.toString());
			xmlManager	= xmlManager.getFirstChild();
			name		= xmlManager.getValue("name");
			Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...getAccountById...name:" + name);
			// Gadget.log.debug("AccountConsumerTask()...xmlAccount: " +
			// xmlAccount.toFormatedString());

		} catch (Exception e) {
			
		}
		return name;
	}
	
	private static String getSystembyObject(Graph database,String objectobjecttype, String objectuniqueidpropertyname, String objectuniqueid) {
		String id ="";
		try {
			String statement = "MATCH (o:"+objectobjecttype+" {"+objectuniqueidpropertyname+":'" + objectuniqueid +
					"'})<-[]-(s:System) RETURN s.systemid AS systemid;";
				Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...getSystembyObject...statement:" + statement);
				XML xmlManager = database.getXML(statement, "Systems", "system");
				Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...getSystembyObject...toString:" + xmlManager.toString());
				xmlManager = xmlManager.getFirstChild();
				id = xmlManager.getValue("systemid");
				Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...getSystembyObject...name:" + id);
		} catch (Exception e) {
			
		}
		return id;
	}
	
	private static String getAuthorisationSystemID(Graph database) {
		String id ="";
		try {
			String statement = "MATCH (d:Daccord)-[:IS_AUTHORIZATIONSYSTEM]->(s:System) RETURN s.systemid AS systemid;";
				Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...getSystembyObject...statement:" + statement);
				XML xmlManager = database.getXML(statement, "Systems", "system");
				Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...getSystembyObject...toString:" + xmlManager.toString());
				xmlManager = xmlManager.getFirstChild();
				id = xmlManager.getValue("systemid");
				Gadget.log.debug("MSDYNAMICSPersonCandidateConsumerTask()...getSystembyObject...name:" + id);
		} catch (Exception e) {
			
		}
		return id;
	}


	// ========================================
	// ACTION HANDLER: DELETE
	// ========================================

	public static void handleDelete(String personCandidateId, Graph database) {

		Gadget.log.info("===========================================");
		Gadget.log.info("PersonCandidateTaskHandler.handleDelete()");
		Gadget.log.info("===========================================");
		Gadget.log.info("  PersonCandidate ID (employeeid): {}", personCandidateId);

		// ========================================
		// 1. Find corresponding Person without leavedate
		// ========================================

		String selectStatement =
			"MATCH (p:Person {employeeid: $employeeid}) " +
			"WHERE p.leavedate IS NULL OR p.leavedate = '' " +
			"RETURN p.globaluniqueid AS guid, p.givenname AS givenname, p.surname AS surname, p.status AS status";

		List<Record> records;
		try {
			HashMap<String, Object> params = new HashMap<>();
			params.put("employeeid", personCandidateId);
			records = database.execute(selectStatement, params);
		} catch (GraphException e) {
			Gadget.log.error("  → Error querying Person for employeeid {}: {}", personCandidateId, e.getMessage(), e);
			return;
		}

		if (records.isEmpty()) {
			Gadget.log.info("  → No Person without leavedate found for employeeid {} – nothing to do.", personCandidateId);
			return;
		}

		// ========================================
		// 2. Set leavedate = today on each found Person
		// ========================================

		String leavedate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) + " 00:00:00";
		Gadget.log.info("  → Setting leavedate = {} on {} Person(s)", leavedate, records.size());

		String updateStatement =
			"MATCH (p:Person {globaluniqueid: $guid}) SET p.leavedate = $leavedate";

		for (Record record : records) {
			String guid     = record.get("guid").asString();
			String name     = record.get("givenname").asString() + " " + record.get("surname").asString();
			String status   = record.get("status").asString();

			Gadget.log.info("  → Person: {} ({}) status={}", name, guid, status);

			try {
				HashMap<String, Object> params = new HashMap<>();
				params.put("guid", guid);
				params.put("leavedate", leavedate);
				database.execute(updateStatement, params);
				Gadget.log.info("  → leavedate set ✓ – Policy 1 will pick up on next run");
			} catch (GraphException e) {
				Gadget.log.error("  → Error setting leavedate for Person {}: {}", guid, e.getMessage(), e);
			}
		}
	}

}
