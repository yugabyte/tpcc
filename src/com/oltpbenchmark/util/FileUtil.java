/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.util;

import java.io.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author pavlo
 */
public abstract class FileUtil {
    /**
     * Given a basename for a file, find the next possible filename if this file
     * already exists. For example, if the file test.res already exists, create
     * a file called, test.1.res
     */
    public static String getNextFilename(String directory, String file_basename, String extension) {
        Path path = Paths.get(directory, file_basename + extension);
        if (!Files.exists(path))
            return path.toString();

        assert path.toFile().isFile();
        // Check how many files already exist
        int counter = 1;
        Path nextPath = Paths.get(directory, file_basename + extension);
        while(Files.exists(nextPath)) {
            ++counter;
            nextPath = Paths.get(directory, file_basename + "." + counter + extension);
        }
        return nextPath.toString();
    }

    public static boolean exists(String path) {
        return (new File(path).exists());
    }

    /**
     * Create any directory in the list paths if it doesn't exist
     */
    public static void makeDirIfNotExists(String... paths) {
        for (String p : paths) {
            if (p == null)
                continue;
            File f = new File(p);
            if (!f.exists()) {
                f.mkdirs();
            }
        } // FOR
    }

}
