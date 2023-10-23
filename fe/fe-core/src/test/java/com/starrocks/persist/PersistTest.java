// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.persist;

import com.starrocks.common.io.Writable;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class PersistTest {
    @Test
    public void testEmptyConstructorOfWritableSubClasses() throws Exception {
        String basePackage = Writable.class.getClassLoader().getResource("").getPath();
        File[] files = new File(basePackage).listFiles();
        List<String> allClassPaths = new ArrayList<>();
        for (File file : files) {
            if (file.isDirectory()) {
                listPackages(file.getName(), allClassPaths);
            }
        }

        for (String classPath : allClassPaths) {
            Class<?> clazz = Class.forName(classPath);
            if (clazz.getSuperclass() == null) {
                continue;
            }

            for (Class<?> superClazz : clazz.getInterfaces()) {
                if (superClazz.equals(Writable.class) && !clazz.isAnonymousClass()) {
                    try {
                        clazz.newInstance();
                    } catch (Throwable t) {
                        System.out.println("class : " + classPath + " should have empty constructor");
                    }
                }
            }
        }
    }

    public void listPackages(String basePackage, List<String> classes) {
        URL url = Writable.class.getClassLoader()
                .getResource("./" + basePackage.replaceAll("\\.", "/"));
        System.out.println("start to ");
        File directory = new File(url.getFile());
        for (File file : directory.listFiles()) {
            if (file.isDirectory()) {
                listPackages(basePackage + "." + file.getName(), classes);
            } else {
                String classpath = file.getName();
                if (classpath.endsWith(".class")) {
                    classes.add(basePackage + "." + classpath.substring(0, classpath.length() - ".class".length()));
                }
            }
        }
    }
}
