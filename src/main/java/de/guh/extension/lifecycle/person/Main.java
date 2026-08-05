package de.guh.extension.lifecycle.person;

import java.io.File;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.guh.gadget.Gadget;
import de.guh.gadget.GadgetConfiguration;
import de.guh.plugin.daccord.DACConnection;
import de.guh.plugin.daccord.DACIntegrationpackage;
import de.guh.plugin.daccord.DACSession;
import de.guh.plugin.daccord.DACWorkflowEngine;
import de.guh.plugin.daccordevent.EventCollectionDTO;
import de.guh.plugin.kafka.KafkaConsumerTask;
import de.guh.plugin.xml.XML;

public class Main {

	protected static final Logger log = LogManager.getRootLogger();

	// ========================================
	// CONSTANTS
	// ========================================

	/**
	 * Fixed Integration Package ID for Lifecycle Extension
	 */
	private static final String INTEGRATIONPACKAGEID = "101";

	public static void main(String[] args) {

		// Reduce verbose logging
		org.apache.logging.log4j.core.config.Configurator.setLevel("io.netty", org.apache.logging.log4j.Level.WARN);
		org.apache.logging.log4j.core.config.Configurator.setLevel("org.neo4j", org.apache.logging.log4j.Level.WARN);
		org.apache.logging.log4j.core.config.Configurator.setLevel("de.guh.plugin.neo4j", org.apache.logging.log4j.Level.WARN);

		String configfile = "";
		String testfile = "";

		// Argument parsing
		if (args == null || args.length == 0) {
			printUsage();
			System.exit(1);
		}

		if (args.length == 1) {
			configfile	= args[0];
			testfile	= null;		// Use default test event
		} else if (args.length == 2) {
			configfile	= args[0];
			testfile	= args[1];
		} else {
			printUsage();
			System.exit(1);
		}

		log.info("===========================================");
		log.info("daccord Person Lifecycle Extension");
		log.info("===========================================");
		log.info("Config: " + configfile);
		log.info("Test File: " + (testfile != null ? testfile : "default test event"));
		log.info("Integration Package ID: " + INTEGRATIONPACKAGEID +
			" (fixed)");
		log.info("===========================================");

		try {

			// ========================================
			// 1. LOAD CONFIGURATION
			// ========================================

			log.info("Loading config.xml...");
			GadgetConfiguration gdConfig = new GadgetConfiguration(configfile, true, log);
			log.info("Config loaded successfully!");

			// Extract SystemDB connection parameters
			log.info("Reading SystemDB connection parameters...");
			String systemdbprotocol = gdConfig.getParameter("systemdbprotocol", "system");
			String systemdbhost = gdConfig.getParameter("systemdbhost", "system");
			String systemdbport = gdConfig.getParameter("systemdbport", "system");
			String systemdbname = gdConfig.getParameter("systemdbname", "system");
			String systemdbuser = gdConfig.getParameter("systemdbuser", "system");
			String systemdbpassword = gdConfig.getParameter("systemdbpassword", "system");

			log.info("SystemDB Protocol: " + systemdbprotocol);
			log.info("SystemDB Host: " + systemdbhost);
			log.info("SystemDB Port: " + systemdbport);
			log.info("SystemDB Name: " + systemdbname);
			log.info("SystemDB User: " + systemdbuser);
			log.info("SystemDB Password: ***");

			// Extract system ID
			String dacsystemid = gdConfig.getParameter("systemid", "system");
			log.info("DAC System ID: " + dacsystemid);

			// ========================================
			// 2. CREATE DACCONNECTION
			// ========================================

			log.info("Creating DACConnection...");
			DACConnection dacconnection = new DACConnection("220", systemdbprotocol, systemdbhost, systemdbport, systemdbname, systemdbuser, systemdbpassword);

			if (dacconnection.hasStatusError()) {
				log.error("Failed to connect to SystemDB!");
				log.error("Status: " + dacconnection.status());
				System.exit(1);
			}

			log.info("DACConnection created successfully!");
			log.info("Connection ready: " + dacconnection.isReady());

			// ========================================
			// 3. CREATE DACSESSION
			// ========================================

			log.info("Creating DACSession...");
			DACSession dacsession = new DACSession(dacconnection, dacsystemid, DACSession.verificationcode);
			log.info("DACSession created successfully!");

			// ========================================
			// 4. LOAD TEST FILE (or use default)
			// ========================================

			log.info("===========================================");
			log.info("Loading test configuration...");

			XML testXml = null;
			String workflowengineid = null;
			EventCollectionDTO eventCollection = null;

			if (testfile != null) {
				// Load from file
				log.info("Loading test file: " + testfile);
				File file = new File(testfile);
				if (!file.exists()) {
					log.error("Test file not found: " + testfile);
					System.exit(1);
				}

				String xmlContent = readFile(testfile);
				testXml = new XML(xmlContent);

				// Extract workflowengineid from <config>
				XML configNode = testXml.findNode("//config");
				if (configNode == null) {
					log.error("No <config> section found in test file!");
					System.exit(1);
				}

				workflowengineid = configNode.getValue("workflowengineid");
				if (workflowengineid == null || workflowengineid.isEmpty()) {
					log.error("No workflowengineid found in <config>!");
					System.exit(1);
				}

				log.info("Workflow Engine ID: " + workflowengineid);

				// Extract event
				XML eventXml = testXml.findNode("//eventCollection");
				if (eventXml == null) {
					log.error("No eventCollection found in test file!");
					System.exit(1);
				}

				eventCollection = new EventCollectionDTO(eventXml);

			} else {
				// Use defaults
				log.info("Using default test configuration");
				workflowengineid	= "1";
				eventCollection		= createDefaultTestEvent();
			}

			log.info("Test configuration loaded successfully!");

			// ========================================
			// 5. LOAD GADGET CONFIG FROM DATABASE
			// ========================================

			log.info("===========================================");
			log.info("Loading Gadget configuration from database...");
			log.info("Using Integration Package ID: " + INTEGRATIONPACKAGEID);

			DACIntegrationpackage dacip = new DACIntegrationpackage(dacsession, INTEGRATIONPACKAGEID, null);

			String gadgetprotocol = dacip.protocol();
			String gadgethost = dacip.host();
			String gadgetport = dacip.port();
			String gadgetpath = dacip.path();

			log.info("Gadget Protocol: " + gadgetprotocol);
			log.info("Gadget Host: " + gadgethost);
			log.info("Gadget Port: " + gadgetport);
			log.info("Gadget Path: " + gadgetpath);

			// Set Gadget static config
			Gadget.PROTOCOL	= gadgetprotocol;
			Gadget.HOST		= gadgethost;
			Gadget.PORT		= gadgetport;
			Gadget.PATH		= gadgetpath;

			log.info("Gadget configuration loaded successfully!");

			// ========================================
			// 6. SETUP WORKFLOW ENGINES
			// ========================================

			log.info("===========================================");
			log.info("Setting up Workflow Engines...");
			HashMap<String, DACWorkflowEngine> hmWorkflowEngines = new HashMap<>();
			DACWorkflowEngine workflowEngine = new DACWorkflowEngine(dacsession, workflowengineid, null);
			hmWorkflowEngines.put(workflowengineid, workflowEngine);

			log.info("Workflow Engine ready: " + workflowEngine.isReady());
			log.info("Workflow Engine type: " + workflowEngine.type());

			if (workflowEngine.type().equalsIgnoreCase(DACWorkflowEngine.type_DACCORD_CAMUNDA)) {
				log.info("Workflow Engine URL: " + workflowEngine.camundaurl());
			}

			// ========================================
			// 7. LOG EVENT DETAILS
			// ========================================

			log.info("===========================================");
			log.info("Event Details:");
			log.info("Stream Name: " + eventCollection.getStreamname());
			log.info("Stream ID: " + eventCollection.getStreamid());
			log.info("Number of Events: " + eventCollection.getEventMap().size());

			// ========================================
			// 8. EXECUTE CONSUMER TASK
			// ========================================

			log.info("===========================================");
			log.info("Creating Consumer Task...");
			KafkaConsumerTask task = new DaccordPersonLifecycleConsumerTask();

			// Set data
			task.setData("log", log);
			task.setData("dacconnection", dacconnection);
			task.setData("dacsession", dacsession);
			task.setData("dacsystemid", dacsystemid);
			task.setData("hmWorkflowEngines", hmWorkflowEngines);
			task.setData("workflowengine", workflowEngine);

			log.info("Executing Consumer Task...");
			task.execute(eventCollection);

			log.info("===========================================");
			log.info("Execution completed!");
			log.info("===========================================");

			// ========================================
			// 9. CLEANUP
			// ========================================

			log.info("Cleaning up...");

			if (dacconnection != null) {
				dacconnection.destroy();
			}

			if (hmWorkflowEngines != null && !hmWorkflowEngines.isEmpty()) {
				hmWorkflowEngines.forEach((id, engine) -> {
					engine.destroy();
				});
			}

			log.info("Cleanup completed!");

		} catch (Exception e) {
			log.error("Error: " + e.getMessage(), e);
			System.exit(1);
		}
	}

	/**
	 * Create default test event (ENTRY scenario)
	 */
	private static EventCollectionDTO createDefaultTestEvent() {
		String xmlStringNodeEvent = "<eventCollection dbuser=\"daccord\" streamname=\"Lifecycle with PersonCandidate\" streamid=\"1011\">" + "<nodeEvent>" +
			"<eventuniqueid>test-entry-default</eventuniqueid>" +
			"<eventname>PersonCandidate</eventname>" +
			"<labels>PersonCandidate</labels>" +
			"<type>MODIFY_NODE</type>" +
			"<timestamp>1724343599695</timestamp>" +
			"<sourcenodeuniqueid>100028</sourcenodeuniqueid>" +
			"<sourcenodename>Max Mustermann</sourcenodename>" +
			"<propertyEvents>" +
			"<propertyEvent>" +
			"<type>MODIFY_PROPERTY</type>" +
			"<name>relevantforadd</name>" +
			"<oldvalue>false</oldvalue>" +
			"<newvalue>true</newvalue>" +
			"</propertyEvent>" +
			"</propertyEvents>" +
			"</nodeEvent>" +
			"</eventCollection>";

		return new EventCollectionDTO(new XML(xmlStringNodeEvent));
	}

	/**
	 * Read file content
	 */
	private static String readFile(String filepath) throws Exception {
		return new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(filepath)));
	}

	private static void printUsage() {
		System.out.println("daccord Person Lifecycle Extension");
		System.out.println("---------------------------------------------");
		System.out.println("usage: java -Dlog4j2.level=DEBUG -jar lifecycle-person-extension.jar <config.xml> [test_event.xml]");
		System.out.println("");
		System.out.println("ARGUMENTS:");
		System.out.println("  config.xml        - Configuration file with SystemDB connection (required)");
		System.out.println("  test_event.xml    - Test file with event and workflow engine ID (optional)");
		System.out.println("");
		System.out.println("CONSTANTS:");
		System.out.println("  Integration Package ID: " + INTEGRATIONPACKAGEID +
			" (fixed in code)");
		System.out.println("");
		System.out.println("TEST FILE FORMAT:");
		System.out.println("  <lifecycle>");
		System.out.println("    <config>");
		System.out.println("      <workflowengineid>1</workflowengineid>");
		System.out.println("    </config>");
		System.out.println("    <eventCollection>...</eventCollection>");
		System.out.println("  </lifecycle>");
		System.out.println("");
		System.out.println("LOG LEVELS:");
		System.out.println("  -Dlog4j2.level=DEBUG   - For detailed troubleshooting");
		System.out.println("  -Dlog4j2.level=INFO    - For normal operation");
		System.out.println("");
		System.out.println("EXAMPLES:");
		System.out.println("  # Test with default ENTRY event");
		System.out.println("  java -Dlog4j2.level=DEBUG -jar lifecycle-person-extension.jar config.xml");
		System.out.println("");
		System.out.println("  # Test ENTRY scenario with custom event");
		System.out.println("  java -Dlog4j2.level=DEBUG -jar lifecycle-person-extension.jar config.xml test_event_entry.xml");
		System.out.println("");
		System.out.println("===========================================");
	}
}