/*
 * Slightly modified version of the com.ibatis.common.jdbc.ScriptRunner class
 * from the iBATIS Apache project. Only removed dependency on Resource class
 * and a constructor 
 */
/*
 *  Copyright 2004 Clinton Begin
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.oltpbenchmark.util;

import java.net.URL;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.*;

import org.apache.log4j.Logger;

/**
 * Tool to run database scripts
 * http://pastebin.com/f10584951
 */
public class ScriptRunner {
    private static final Logger LOG = Logger.getLogger(ScriptRunner.class);

	private static final String DEFAULT_DELIMITER = ";";

	private final Connection connection;

	private final boolean stopOnError;
	private final boolean autoCommit;

	private final String delimiter = DEFAULT_DELIMITER;
	private final boolean fullLineDelimiter = false;

	/**
	 * Default constructor
	 */
	public ScriptRunner(Connection connection, boolean autoCommit,
			boolean stopOnError) {
		this.connection = connection;
		this.autoCommit = autoCommit;
		this.stopOnError = stopOnError;
	}

	/**
	 * Runs an SQL script
	 */
	public void runScript(URL resource) throws IOException, SQLException {
		Reader reader = new InputStreamReader(resource.openStream());
		try {
			boolean originalAutoCommit = connection.getAutoCommit();
			try {
				if (originalAutoCommit != this.autoCommit) {
					connection.setAutoCommit(this.autoCommit);
				}
				runScript(connection, reader);
			} finally {
				connection.setAutoCommit(originalAutoCommit);
			}
		} catch (IOException | SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException("Error running script.  Cause: " + e, e);
		}
	}

	/**
	 * Runs an SQL script (read in using the Reader parameter) using the
	 * connection passed in
	 * 
	 * @param conn
	 *            - the connection to use for the script
	 * @param reader
	 *            - the source of the script
	 * @throws SQLException
	 *             if any SQL errors occur
	 * @throws IOException
	 *             if there is an error reading from the Reader
	 */
	private void runScript(Connection conn, Reader reader) throws IOException, SQLException {
		StringBuffer command = null;
		try {
			LineNumberReader lineReader = new LineNumberReader(reader);
			String line;
			while ((line = lineReader.readLine()) != null) {
			    if (LOG.isDebugEnabled()) LOG.debug(line);
				if (command == null) {
					command = new StringBuffer();
				}
				String trimmedLine = line.trim();
				if (trimmedLine.startsWith("--")) {
					LOG.debug(trimmedLine);
				} else if (trimmedLine.length() < 1
						|| trimmedLine.startsWith("//")) {
					// Do nothing
				} else if (!fullLineDelimiter
						&& trimmedLine.endsWith(getDelimiter())
						|| fullLineDelimiter
						&& trimmedLine.equals(getDelimiter())) {
					command.append(line, 0, line.lastIndexOf(getDelimiter()));
					command.append(" ");
					Statement statement = conn.createStatement();

					boolean hasResults = false;
					final String sql = command.toString().trim();
					if (stopOnError) {
						hasResults = statement.execute(sql);
					} else {
						try {
							statement.execute(sql);
						} catch (SQLException e) {
							System.err.println("Error executing: " + sql);
							System.err.println(e.toString());
						}
					}

					if (autoCommit && !conn.getAutoCommit()) {
						conn.commit();
					}
					
					// HACK
					if (hasResults && !sql.toUpperCase().startsWith("CREATE")) {
    					ResultSet rs = statement.getResultSet();
    					if (rs != null) {
    						ResultSetMetaData md = rs.getMetaData();
    						int cols = md.getColumnCount();
    						for (int i = 0; i < cols; i++) {
    							String name = md.getColumnLabel(i);
								System.out.println(name + "\t");
    						}
							System.out.println();
    						while (rs.next()) {
    							for (int i = 0; i < cols; i++) {
    								String value = rs.getString(i);
    								System.out.println(value + "\t");
    							}
								System.out.println();
    						}
    						rs.close();
    					}
					}

					command = null;
					try {
						statement.close();
					} catch (Exception e) {
						// Ignore to workaround a bug in Jakarta DBCP
					}
					Thread.yield();
				} else {
					command.append(line);
					command.append(" ");
				}
			}
			if (!autoCommit) {
				conn.commit();
			}
		} catch (SQLException | IOException e) {
			System.err.println("Error executing: " + command);
			throw e;
		} finally {
			if (!autoCommit) conn.rollback();
			flush();
		}
	}

	private String getDelimiter() {
		return delimiter;
	}

	private void flush() {
		System.out.flush();
		System.err.flush();
	}
}
