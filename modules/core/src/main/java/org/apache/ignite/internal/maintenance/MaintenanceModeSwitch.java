/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.maintenance;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/** */
public class MaintenanceModeSwitch {
    /** */
    public static final String MAINTENANCE_DESCRIPTOR = "maintenance.desc";

    private List<MaintenanceTarget> targets = new ArrayList<>();

    public MaintenanceModeSwitch() {
    }

    public MaintenanceModeSwitch(List<MaintenanceTarget> targets) {
        this.targets.addAll(targets);
    }

    /** */
    public boolean maintenanceOfType(MaintenanceType type) {
        return targets.stream().anyMatch(target -> target.type() == type);
    }

    public static MaintenanceModeSwitch checkMaintenace(String igniteWorkDir, boolean persistenceEnabled) {
        if (!persistenceEnabled)
            return new MaintenanceModeSwitch();

        Path mntcDescPath = Paths.get(igniteWorkDir).resolve(MAINTENANCE_DESCRIPTOR);

        if (!Files.exists(mntcDescPath))
            return new MaintenanceModeSwitch();

        File mntcDesc = mntcDescPath.toFile();

        List<MaintenanceTarget> targets = new ArrayList<>();

        try (FileInputStream in = new FileInputStream(mntcDesc)) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String mntcLine;

                MaintenanceTargetFactory factory = MaintenanceTargetFactory.instance();

                while ((mntcLine = reader.readLine()) != null) {
                    // TODO debug logging about reading maintenance file

                    try {
                        targets.add(factory.createFromDescriptor(mntcLine));
                    }
                    catch (IllegalArgumentException e) {
                        // TODO info or warning about broken format of particular target
                    }
                }
            }
        }
        catch (IOException e) {
            //TODO pass in a logger to warn user about IO when reading maintenance descriptor

            return new MaintenanceModeSwitch();
        }

        return new MaintenanceModeSwitch(targets);
    }

    //TODO remove when quick and dirty phase of development is over
    public void addTarget(MaintenanceTarget target) {
        targets.add(target);
    }
}
