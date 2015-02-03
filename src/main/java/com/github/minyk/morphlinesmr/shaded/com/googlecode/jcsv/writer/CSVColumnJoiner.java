package com.github.minyk.morphlinesmr.shaded.com.googlecode.jcsv.writer;

import com.github.minyk.morphlinesmr.shaded.com.googlecode.jcsv.CSVStrategy;

/**
 * The csv column joiner receives an array of strings and joines it
 * to a single string that represents a line of the csv file.
 *
 */
public interface CSVColumnJoiner {
	/**
	 * Converts a String[] array into a single string, using the
	 * given csv strategy.
	 *
	 * @param data incoming data
	 * @param strategy the csv format descriptor
	 * @return the joined columns
	 */
	public String joinColumns(String[] data, CSVStrategy strategy);
}
