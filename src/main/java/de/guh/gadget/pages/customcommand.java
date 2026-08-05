package de.guh.gadget.pages;

import de.guh.gadget.GadgetPage;
import de.guh.gadget.GadgetRequest;
import de.guh.gadget.GadgetResponse;
import de.guh.gadget.GadgetSession;


public class customcommand implements GadgetPage {

	@Override
	public void defaultmethod(GadgetRequest gdRequest, GadgetSession gdSession, GadgetResponse gdResponse) {

	}

	public void getDefaultData(GadgetRequest gdRequest, GadgetSession gdSession, GadgetResponse gdResponse) {

		defaultmethod(gdRequest, gdSession, gdResponse);

	}
	public void getCities(GadgetRequest gdRequest, GadgetSession gdSession, GadgetResponse gdResponse) {
		
		/*
		 <inchorus-select id="inchorus-selects-cities">
			<xsl:attribute name="preselect">11</xsl:attribute>
			<xsl:attribute name="validate">.+</xsl:attribute>
			<xsl:attribute name="name">inc-wf-citie</xsl:attribute>
			<xsl:attribute name="hascontentfile">true</xsl:attribute>
			<xsl:attribute name="command">getCities</xsl:attribute>
			<xsl:attribute name="alternativepage">customcommand</xsl:attribute>
			<xsl:attribute name="loglevel">debug</xsl:attribute>				
		  </inchorus-select>
		*/
		gdRequest.log.info("...[running]");
		
	}

}
