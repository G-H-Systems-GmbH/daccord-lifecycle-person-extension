package de.guh.extension.lifecycle.person;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import de.guh.gadget.Gadget;

public class PersonTaskHandler {

	// ========================================
	// PERSON: STATUS_TO_INACTIVE
	// ========================================

	public static void processStatusToInactiveActions(Set<String> eventIds, Map<String, HashMap<String, String>> allData) {

		int counter = 1;
		for (String guid : eventIds) {
			HashMap<String, String> hmData = allData.get(guid);
			if (hmData == null) {
				Gadget.log.error("STATUS_TO_INACTIVE {}/{}: No data for {}", counter++, eventIds.size(), guid);
				continue;
			}

			Gadget.log.info("STATUS_TO_INACTIVE {}/{}: {} {}", counter++, eventIds.size(), hmData.get("givenname"), hmData.get("surname"));

			// TODO: Provisioning Logic
			Gadget.log.info("  → TODO: Query all linked accounts and deactivate");
			Gadget.log.info("  → MATCH (p:Person {{globaluniqueid:$guid}})-[:HAS_ACCOUNT]->(a:Account)");
			Gadget.log.info("  → For each Account: ProvisioningRequest(modifyAccountStatus, inactive)");
		}
	}

	// ========================================
	// PERSON: STATUS_TO_ABSENCE
	// ========================================

	public static void processStatusToAbsenceActions(Set<String> eventIds, Map<String, HashMap<String, String>> allData) {

		int counter = 1;
		for (String guid : eventIds) {
			HashMap<String, String> hmData = allData.get(guid);
			if (hmData == null) {
				Gadget.log.error("STATUS_TO_ABSENCE {}/{}: No data for {}", counter++, eventIds.size(), guid);
				continue;
			}

			Gadget.log.info("STATUS_TO_ABSENCE {}/{}: {} {} (Absence: {} - {})", counter++, eventIds.size(), hmData.get("givenname"), hmData.get("surname"),
					hmData.get("absencestartdate"), hmData.get("absenceenddate"));

			// TODO: Provisioning Logic (Optional)
			Gadget.log.info("  → TODO (Optional): Temporarily lock accounts during absence");
			Gadget.log.info("  → Note: Unlock needed when absence ends (via Policy!)");
		}
	}

	// ========================================
	// PERSON: NAME_CHANGE
	// ========================================

	public static void processNameChangeActions(Set<String> eventIds, Map<String, HashMap<String, String>> allData) {

		int counter = 1;
		for (String guid : eventIds) {
			HashMap<String, String> hmData = allData.get(guid);
			if (hmData == null) {
				Gadget.log.error("NAME_CHANGE {}/{}: No data for {}", counter++, eventIds.size(), guid);
				continue;
			}

			Gadget.log.info("NAME_CHANGE {}/{}: {} {}", counter++, eventIds.size(), hmData.get("firstname"), hmData.get("lastname"));

			// TODO: Provisioning Logic
			Gadget.log.info("  → TODO: Update name in all linked accounts");
			Gadget.log.info("  → For each Account: ProvisioningRequest(modifyAccount, firstname/lastname)");
		}
	}

	// ========================================
	// PERSON: ORG_CHANGE
	// ========================================

	public static void processOrgChangeActions(Set<String> eventIds, Map<String, HashMap<String, String>> allData) {

		int counter = 1;
		for (String guid : eventIds) {
			HashMap<String, String> hmData = allData.get(guid);
			if (hmData == null) {
				Gadget.log.error("ORG_CHANGE {}/{}: No data for {}", counter++, eventIds.size(), guid);
				continue;
			}

			Gadget.log.info("ORG_CHANGE {}/{}: {} {} (Dept: {}, Company: {})", counter++, eventIds.size(), hmData.get("givenname"), hmData.get("surname"),
					hmData.get("department"), hmData.get("company"));

			// TODO: Provisioning Logic
			Gadget.log.info("  → TODO: Update org data in all linked accounts");
			Gadget.log.info("  → For each Account: ProvisioningRequest(modifyAccount, dept/company/location)");
		}
	}

	// ========================================
	// PERSON: RESPONSIBLE_CHANGE
	// ========================================

	public static void processResponsibleChangeActions(Set<String> eventIds, Map<String, HashMap<String, String>> allData) {

		int counter = 1;
		for (String guid : eventIds) {
			HashMap<String, String> hmData = allData.get(guid);
			if (hmData == null) {
				Gadget.log.error("RESPONSIBLE_CHANGE {}/{}: No data for {}", counter++, eventIds.size(), guid);
				continue;
			}

			Gadget.log.info("RESPONSIBLE_CHANGE {}/{}: {} {} (Manager: {})", counter++, eventIds.size(), hmData.get("givenname"), hmData.get("surname"),
					hmData.get("manager"));

			// TODO: Provisioning Logic
			Gadget.log.info("  → TODO: Update manager in all linked accounts");
			Gadget.log.info("  → For each Account: ProvisioningRequest(modifyAccount, manager)");
		}
	}

	// ========================================
	// PERSON: BASISDATEN_CHANGE
	// ========================================

	public static void processBasisdataChangeActions(Set<String> eventIds, Map<String, HashMap<String, String>> allData) {

		int counter = 1;
		for (String guid : eventIds) {
			HashMap<String, String> hmData = allData.get(guid);
			if (hmData == null) {
				Gadget.log.error("BASISDATEN_CHANGE {}/{}: No data for {}", counter++, eventIds.size(), guid);
				continue;
			}

			Gadget.log.info("BASISDATEN_CHANGE {}/{}: {} {} (Email: {})", counter++, eventIds.size(), hmData.get("givenname"), hmData.get("surname"),
					hmData.get("email"));

			// TODO: Provisioning Logic
			Gadget.log.info("  → TODO: Update basic data in all linked accounts");
			Gadget.log.info("  → For each Account: ProvisioningRequest(modifyAccount, email/phone/title)");
		}
	}

	// ========================================
	// PERSON: LEAVEDATE_CHANGE
	// ========================================

	public static void processLeavedateChangeActions(Set<String> eventIds, Map<String, HashMap<String, String>> allData) {

		int counter = 1;
		for (String guid : eventIds) {
			HashMap<String, String> hmData = allData.get(guid);
			if (hmData == null) {
				Gadget.log.error("LEAVEDATE_CHANGE {}/{}: No data for {}", counter++, eventIds.size(), guid);
				continue;
			}

			Gadget.log.info("LEAVEDATE_CHANGE {}/{}: {} {} (Leavedate: {})", counter++, eventIds.size(), hmData.get("givenname"), hmData.get("surname"),
					hmData.get("leavedate"));

			// TODO: Decision needed - Process immediately or wait for Policy?
			Gadget.log.info("  → Option 1: Immediate - Set status=inactive, deactivate accounts NOW");
			Gadget.log.info("  → Option 2: Wait - Policy triggers at leavedate");
			Gadget.log.info("  → Decision pending!");
		}
	}

}
