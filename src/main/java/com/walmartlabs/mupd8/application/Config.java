/**
 * Copyright 2011-2012 @WalmartLabs, a division of Wal-Mart Stores, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.walmartlabs.mupd8.application;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class Config {

	public HashMap<String, JSONObject> workerJSONs = new HashMap<String, JSONObject>(); // mapping from performer name to performer json
	// Config.value is no longer supported;
	// java.util.Logger should automatically enforce the correct debugging level.
	public String debug = null;

	JSONObject configuration = new JSONObject();

	// dummy constructor
	public Config () {
	}

	public Config(String sys_config, String app_config) throws Exception {
		loadSysConfig(sys_config);
		loadAppConfig(app_config);
	}
	public Config(File[] configFiles) throws IOException {
		applyFiles(configuration, configFiles);
		workerJSONs = extractWorkerJSONs(configuration);
	}
	public Config(File directory) throws IOException {
		File[] files = directory.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".cfg");
			}
		});
		if (files == null) {
			throw new FileNotFoundException("Configuration path "+directory.toString()+" is not a directory.");
		}
		Arrays.sort(files, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		applyFiles(configuration, files);
		workerJSONs = extractWorkerJSONs(configuration);
	}

	private JSONObject applyFiles(JSONObject destination, File... configFiles) throws IOException {
		for (File file : configFiles) {
			Reader reader = new FileReader(file);
			// TODO Switch to try (Reader reader = new FileReader(file)) { ... } for Java 7.
			try {
				JSONObject parsedFile = (JSONObject)JSONValue.parse(readWithPreprocessing(reader));
				apply(destination, parsedFile);
			} finally {
				if (reader != null) {
					reader.close();
				}
			}
		}
		return destination;
	}

	@SuppressWarnings("unchecked")
	private static void apply(JSONObject destination, JSONObject source) {
		for (Object k : source.keySet()) {
			String key = (String) k;
			if (destination.containsKey(key) && (destination.get(key) != null) && (destination.get(key) instanceof JSONObject) && (source.get(key) instanceof JSONObject)) {
				try {
					JSONObject subDestination = (JSONObject) destination.get(key);
					JSONObject subSource = (JSONObject) source.get(key);
					apply(subDestination, subSource);
				} catch (ClassCastException e) {
					throw new ClassCastException("Config: cannot override key "+key+" with new value because it is not a JSON object");
				}
			} else {
				destination.put(key, source.get(key));
			}
		}
	}

	public static Object getScopedValue(JSONObject object, String[] path) {
		if (path == null) {
			throw new NullPointerException("path parameter to getScopedValue may not be null");
		}
		if (path.length == 0) {
			return object;
		}
		String key = path[0];
		if (!object.containsKey(key)) {
			// TODO ...or return new JSONObject()?
			return null;
		}
		if (path.length == 1) {
			return object.get(key); // not necessarily JSONObject
		}
		return getScopedValue((JSONObject) object.get(key), Arrays.copyOfRange(path, 1, path.length));
	}

    public Object getScopedValue(String[] path) {
        return getScopedValue(this.configuration, path);
    }

	private static HashMap<String, JSONObject> extractWorkerJSONs(JSONObject configuration) {
		HashMap<String, JSONObject> performers = new HashMap<String, JSONObject>();

		JSONObject applications = (JSONObject) getScopedValue(configuration, new String[] { "mupd8", "application" });
		if (applications == null) {
			// Lost cause: No applications found.
			System.err.println("No mupd8:application found; Config.workerJSONs not set.");
			return performers;
		}
		Set<?> applicationNames = applications.keySet();
		if (applicationNames.size() != 1) {
			System.err.println("Exactly one application definition expected, but got "+Integer.toString(applicationNames.size())+" instead; Config.workerJSONs not set.");
			return performers;
		}
		String applicationName = applicationNames.toArray(new String[]{})[0];
		JSONObject performerSection = (JSONObject) getScopedValue(applications, new String[] { applicationName, "performers" });

		for (Object k : performerSection.keySet()) {
			String key = (String) k;
			performers.put(key, (JSONObject) performerSection.get(key));
		}

		return performers;
	}

	// Call before loadAppConfig.
	@SuppressWarnings("unchecked")
	private void loadSysConfig(String filename) throws IOException, Exception {
		JSONObject sysJson = (JSONObject)JSONValue.parse(readWithPreprocessing(new FileReader(filename)));
		configuration = new JSONObject();
		/*
		if (!configuration.containsKey("mupd8")) {
			configuration.put("mupd8", new JSONObject());
		}
		JSONObject mupd8 = configuration.get("mupd8");
		for (String k : new String[]{ "mupd8_status", "slate_store" }) {
			// ...
		}
		*/

		configuration.put("mupd8", sysJson);
	}

	// Call only after loadSysConfig.
	@SuppressWarnings("unchecked")
	private void loadAppConfig(String filename) throws Exception {
		JSONObject appJson = (JSONObject)JSONValue.parse(readWithPreprocessing(new FileReader(filename)));

		JSONObject applicationNameObject = new JSONObject();
		String applicationName = (String) appJson.get("application");
		applicationNameObject.put(applicationName, appJson);

		JSONObject mupd8 = (JSONObject) configuration.get("mupd8");
		mupd8.put("application", applicationNameObject);

		if (appJson.containsKey("performers")) {
			JSONArray performers = (JSONArray)appJson.get("performers");
			for (int i = 0; i < performers.size(); i++) {
				JSONObject json = (JSONObject)performers.get(i);

				String performer = (String)json.get("performer");
				workerJSONs.put(performer, json);
			 }

		}
	}

	/** Read lines, interpreting #include and ignoring ^# (comments).
	 *
	 *  cf. sub Mupd8::Mupd8Config::cpp($filename, $lines)
	 *
	 *  java.nio.file.Files, which implements glob, requires Java 7.
	 *  Without it, this Java implementation fails to #include globs.
	 *
	 * @param input
	 * @return preprocessed file
	 */
	private static String readWithPreprocessing(Reader input) throws IOException {
		BufferedReader bufferedReader = new BufferedReader(input);
		String content = "";

		String line;
		while ((line = bufferedReader.readLine()) != null) {
			if (line.startsWith("#include ")) {
				String filePattern = line.substring("#include ".length());
				if (filePattern.length() < 2) {
					// FIXME Handle the error.
					System.err.println("#include line missing parameter: "+line);
					continue;
				} else {
					if ((filePattern.charAt(0) == '"' && filePattern.charAt(filePattern.length()-1) == '"') ||
						(filePattern.charAt(0) == '<' && filePattern.charAt(filePattern.length()-1) == '>')) {
						filePattern = filePattern.substring(1, filePattern.length() - 1);
					} else {
						// FIXME Handle the error.
						System.err.println("#include parameter has no recognized delimiters \"\" or <>: "+line);
						continue;
					}
				}

				if (filePattern.contains("*") || filePattern.contains("[")) {
					// FIXME Handle the error.
					System.err.println("#include parameter has wildcards, not supported in the JDK until Java 7: "+line);
					continue;
				}

				FileReader file = new FileReader(filePattern);
				content += readWithPreprocessing(file);

			} else if (line.startsWith("#")) {
				continue;
			} else {
				content += line + System.getProperty("line.separator");
			}
		}

		return content;
	}
}
