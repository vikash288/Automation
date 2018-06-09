package com.pricedotcom.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MiscellaneousUtil {

	/**
	 * @param retailerName
	 * @return
	 */
	public String cleanSpecialCharacter(String retailerName) {
		Pattern pt = Pattern.compile("[^a-zA-Z0-9]");
		Matcher match = pt.matcher(retailerName);
		while (match.find()) {
			String s = match.group();
			retailerName = retailerName.replaceAll("\\" + s, "");
		}
		return retailerName.toUpperCase();
	}
	
}
