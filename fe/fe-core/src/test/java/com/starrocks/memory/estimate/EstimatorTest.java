// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.memory.estimate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EstimatorTest {

    @BeforeEach
    void setUp() {
        // Clear all caches and registries before each test
        Estimator.SHALLOW_SIZE_CACHE.clear();
        Estimator.CUSTOM_ESTIMATORS.clear();
        Estimator.SHALLOW_MEMORY_CLASSES.clear();
        Estimator.CLASS_NESTED_FIELDS.clear();
    }

    @AfterEach
    void tearDown() {
        // Clear all caches and registries after each test
        Estimator.SHALLOW_SIZE_CACHE.clear();
        Estimator.CUSTOM_ESTIMATORS.clear();
        Estimator.SHALLOW_MEMORY_CLASSES.clear();
        Estimator.CLASS_NESTED_FIELDS.clear();
    }

    // ==================== Null and Basic Tests ====================

    @Test
    void testEstimateNull() {
        assertEquals(0, Estimator.estimate(null));
    }

    @Test
    void testShallowNull() {
        // Explicitly cast to Object to call shallow(Object) instead of shallow(Class<?>)
        assertEquals(0, Estimator.shallow((Object) null));
    }

    // ==================== Primitive Array Tests ====================

    @Test
    void testEstimatePrimitiveIntArray() {
        int[] array = new int[100];
        long size = Estimator.estimate(array);
        // Array header (16) + 100 * 4 bytes = 416
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100 * 4, size);
    }

    @Test
    void testEstimatePrimitiveLongArray() {
        long[] array = new long[50];
        long size = Estimator.estimate(array);
        // Array header (16) + 50 * 8 bytes = 416
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 50 * 8, size);
    }

    @Test
    void testEstimatePrimitiveByteArray() {
        byte[] array = new byte[200];
        long size = Estimator.estimate(array);
        // Array header (16) + 200 * 1 byte = 216
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 200, size);
    }

    @Test
    void testEstimatePrimitiveBooleanArray() {
        boolean[] array = new boolean[100];
        long size = Estimator.estimate(array);
        // Array header (16) + 100 * 1 byte = 116
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100, size);
    }

    @Test
    void testEstimatePrimitiveCharArray() {
        char[] array = new char[100];
        long size = Estimator.estimate(array);
        // Array header (16) + 100 * 2 bytes = 216
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100 * 2, size);
    }

    @Test
    void testEstimatePrimitiveShortArray() {
        short[] array = new short[100];
        long size = Estimator.estimate(array);
        // Array header (16) + 100 * 2 bytes = 216
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100 * 2, size);
    }

    @Test
    void testEstimatePrimitiveFloatArray() {
        float[] array = new float[100];
        long size = Estimator.estimate(array);
        // Array header (16) + 100 * 4 bytes = 416
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100 * 4, size);
    }

    @Test
    void testEstimatePrimitiveDoubleArray() {
        double[] array = new double[100];
        long size = Estimator.estimate(array);
        // Array header (16) + 100 * 8 bytes = 816
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100 * 8, size);
    }

    @Test
    void testEstimateEmptyPrimitiveArray() {
        int[] array = new int[0];
        long size = Estimator.estimate(array);
        assertEquals(Estimator.ARRAY_HEADER_SIZE, size);
    }

    // ==================== Object Array Tests ====================

    @Test
    void testEstimateEmptyObjectArray() {
        Object[] array = new Object[0];
        long size = Estimator.estimate(array);
        assertEquals(Estimator.ARRAY_HEADER_SIZE, size);
    }

    @Test
    void testEstimateObjectArrayWithNulls() {
        Object[] array = new Object[10];
        // All null elements
        long size = Estimator.estimate(array);
        // Array header (16) + 10 * reference size (4) = 56
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 10 * 4, size);
    }

    @Test
    void testEstimateStringArray() {
        String[] array = new String[] {"hello", "world", "test"};
        long size = Estimator.estimate(array);
        // Should be positive and include array overhead + string sizes
        assertTrue(size > Estimator.ARRAY_HEADER_SIZE + 3 * 4);
    }

    // ==================== Collection Tests ====================

    @Test
    void testEstimateEmptyArrayList() {
        List<String> list = new ArrayList<>();
        long size = Estimator.estimate(list);
        assertTrue(size > 0);
    }

    @Test
    void testEstimateArrayListWithElements() {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add("element" + i);
        }
        long size = Estimator.estimate(list);
        // Should be significantly larger than empty list
        assertTrue(size > Estimator.estimate(new ArrayList<>()));
    }

    @Test
    void testEstimateLinkedList() {
        LinkedList<Integer> list = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            list.add(i);
        }
        long size = Estimator.estimate(list);
        assertTrue(size > 0);
    }

    @Test
    void testEstimateHashSet() {
        Set<String> set = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            set.add("item" + i);
        }
        long size = Estimator.estimate(set);
        assertTrue(size > 0);
    }

    @Test
    void testEstimateConcurrentHashMap() {
        Map<String, Integer> map = new ConcurrentHashMap<>();
        for (int i = 0; i < 50; i++) {
            map.put("key" + i, i);
        }
        long size = Estimator.estimate(map);
        assertTrue(size > 0);
    }

    // ==================== Map Tests ====================

    @Test
    void testEstimateEmptyHashMap() {
        Map<String, String> map = new HashMap<>();
        long size = Estimator.estimate(map);
        assertTrue(size > 0);
    }

    @Test
    void testEstimateHashMapWithElements() {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            map.put("key" + i, "value" + i);
        }
        long size = Estimator.estimate(map);
        assertTrue(size > Estimator.estimate(new HashMap<>()));
    }

    // ==================== Custom Object Tests ====================

    @Test
    void testEstimateSimpleObject() {
        SimpleObject obj = new SimpleObject(42, "test");
        long size = Estimator.estimate(obj);
        assertTrue(size > 0);
    }

    @Test
    void testEstimateNestedObject() {
        NestedObject nested = new NestedObject();
        nested.inner = new SimpleObject(1, "inner");
        long size = Estimator.estimate(nested);
        // Should include both outer and inner object
        assertTrue(size > Estimator.shallow(nested));
    }

    @Test
    void testEstimateObjectWithCollection() {
        ObjectWithCollection obj = new ObjectWithCollection();
        for (int i = 0; i < 50; i++) {
            obj.items.add("item" + i);
        }
        long size = Estimator.estimate(obj);
        assertTrue(size > Estimator.shallow(obj));
    }

    // ==================== Max Depth Tests ====================

    @Test
    void testEstimateWithDepthZero() {
        NestedObject nested = new NestedObject();
        nested.inner = new SimpleObject(1, "inner");

        long shallowSize = Estimator.estimate(nested, 0);
        long deepSize = Estimator.estimate(nested, 8);

        // With depth 0, should only return shallow size
        assertEquals(Estimator.shallow(nested), shallowSize);
        assertTrue(deepSize > shallowSize);
    }

    @Test
    void testEstimateWithCustomDepth() {
        DeeplyNestedObject deep = createDeeplyNestedObject(10);

        long depth2 = Estimator.estimate(deep, 2);
        long depth5 = Estimator.estimate(deep, 5);
        long depth10 = Estimator.estimate(deep, 10);

        // More depth should generally result in larger size estimation
        assertTrue(depth5 >= depth2);
        assertTrue(depth10 >= depth5);
    }

    // ==================== Sample Size Tests ====================

    @Test
    void testEstimateWithCustomSampleSize() {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            list.add("element" + i);
        }

        long sample1 = Estimator.estimate(list, 8, 1);
        long sample5 = Estimator.estimate(list, 8, 5);
        long sample10 = Estimator.estimate(list, 8, 10);

        // All should be positive
        assertTrue(sample1 > 0);
        assertTrue(sample5 > 0);
        assertTrue(sample10 > 0);
    }

    // ==================== Annotation Tests ====================

    @Test
    void testIgnoreMemoryTrackOnClass() {
        IgnoredClass obj = new IgnoredClass();
        obj.data = new int[1000];
        assertEquals(0, Estimator.estimate(obj));
    }

    @Test
    void testIgnoreMemoryTrackOnField() {
        ObjectWithIgnoredField obj = new ObjectWithIgnoredField();
        obj.tracked = "tracked";
        obj.ignored = new int[1000];

        long size = Estimator.estimate(obj);
        // Size should not include the ignored array
        long arraySize = Estimator.ARRAY_HEADER_SIZE + 1000 * 4;
        assertTrue(size < arraySize);
    }

    @Test
    void testShallowMemoryAnnotation() {
        ShallowOnlyClass obj = new ShallowOnlyClass();
        obj.data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            obj.data.add("item" + i);
        }

        long size = Estimator.estimate(obj);
        long shallowSize = Estimator.shallow(obj);
        // Should only return shallow size
        assertEquals(shallowSize, size);
    }

    // ==================== Custom Estimator Tests ====================

    @Test
    void testRegisterAndUseCustomEstimator() {
        Estimator.registerCustomEstimator(CustomEstimatedClass.class, obj -> 12345L);

        CustomEstimatedClass obj = new CustomEstimatedClass();
        assertEquals(12345L, Estimator.estimate(obj));
    }

    @Test
    void testGetCustomEstimator() {
        CustomEstimator estimator = obj -> 100L;
        Estimator.registerCustomEstimator(CustomEstimatedClass.class, estimator);

        assertEquals(estimator, Estimator.getCustomEstimator(CustomEstimatedClass.class));
        assertNull(Estimator.getCustomEstimator(SimpleObject.class));
    }

    @Test
    void testStringEstimator() {
        Estimator.registerCustomEstimator(String.class, new StringEstimator());

        String str = "Hello, World!";
        long size = Estimator.estimate(str);

        // Should be shallow size + array header + string length
        long expected = Estimator.shallow(str) + Estimator.ARRAY_HEADER_SIZE + str.length();
        assertEquals(expected, size);
    }

    // ==================== Shallow Memory Class Registration Tests ====================

    @Test
    void testRegisterShallowMemoryClass() {
        assertFalse(Estimator.isShallowMemoryClass(SimpleObject.class));

        Estimator.registerShallowMemoryClass(SimpleObject.class);

        assertTrue(Estimator.isShallowMemoryClass(SimpleObject.class));
    }

    @Test
    void testIsShallowMemoryClassWithAnnotation() {
        assertTrue(Estimator.isShallowMemoryClass(ShallowOnlyClass.class));
    }

    @Test
    void testCollectionWithShallowMemoryElements() {
        Estimator.registerShallowMemoryClass(SimpleObject.class);

        List<SimpleObject> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(new SimpleObject(i, "name" + i));
        }

        long size = Estimator.estimate(list);
        // Size should be list shallow size + element shallow size * count
        long listShallow = Estimator.shallow(list);
        long elementShallow = Estimator.shallow(SimpleObject.class);
        long expected = listShallow + elementShallow * 100;
        assertEquals(expected, size);
    }

    // ==================== Shallow Size Tests ====================

    @Test
    void testShallowSizeObject() {
        SimpleObject obj = new SimpleObject(1, "test");
        long shallow = Estimator.shallow(obj);
        assertTrue(shallow > 0);
    }

    @Test
    void testShallowSizeClass() {
        long shallow = Estimator.shallow(SimpleObject.class);
        assertTrue(shallow > 0);
    }

    @Test
    void testShallowSizeArrayThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            Estimator.shallow(int[].class);
        });
    }

    @Test
    void testShallowSizeArrayInstance() {
        int[] array = new int[100];
        long shallow = Estimator.shallow(array);
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100 * 4, shallow);
    }

    // ==================== Cache Tests ====================

    @Test
    void testShallowSizeIsCached() {
        SimpleObject obj = new SimpleObject(1, "test");

        assertTrue(Estimator.SHALLOW_SIZE_CACHE.isEmpty());

        Estimator.shallow(obj);

        assertTrue(Estimator.SHALLOW_SIZE_CACHE.containsKey(SimpleObject.class));
    }

    @Test
    void testPrimitiveArrayShallowSizeNotCached() {
        int[] intArray = new int[100];
        long[] longArray = new long[50];
        byte[] byteArray = new byte[200];

        assertTrue(Estimator.SHALLOW_SIZE_CACHE.isEmpty());

        // Calculate shallow size for primitive arrays
        long intSize = Estimator.shallow(intArray);
        long longSize = Estimator.shallow(longArray);
        long byteSize = Estimator.shallow(byteArray);

        // Verify correct sizes are returned
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100 * 4, intSize);
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 50 * 8, longSize);
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 200, byteSize);

        // Verify primitive arrays are NOT cached (because each length produces different size)
        assertTrue(Estimator.SHALLOW_SIZE_CACHE.isEmpty());
        assertFalse(Estimator.SHALLOW_SIZE_CACHE.containsKey(int[].class));
        assertFalse(Estimator.SHALLOW_SIZE_CACHE.containsKey(long[].class));
        assertFalse(Estimator.SHALLOW_SIZE_CACHE.containsKey(byte[].class));
    }

    @Test
    void testNestedFieldsAreCached() {
        NestedObject obj = new NestedObject();
        obj.inner = new SimpleObject(1, "test");

        assertTrue(Estimator.CLASS_NESTED_FIELDS.isEmpty());

        Estimator.estimate(obj);

        assertTrue(Estimator.CLASS_NESTED_FIELDS.containsKey(NestedObject.class));
    }

    @Test
    void testNestedFieldsContainsCorrectFieldNames() {
        // Test SimpleObject: should only contain "name" (id is primitive, skipped)
        SimpleObject simpleObj = new SimpleObject(1, "test");
        Estimator.estimate(simpleObj);

        List<Field> simpleFields = Estimator.CLASS_NESTED_FIELDS.get(SimpleObject.class);
        assertNotNull(simpleFields);
        assertEquals(1, simpleFields.size());
        assertEquals("name", simpleFields.get(0).getName());
    }

    @Test
    void testNestedFieldsForNestedObject() {
        // Test NestedObject: should contain "inner"
        NestedObject nestedObj = new NestedObject();
        nestedObj.inner = new SimpleObject(1, "test");
        Estimator.estimate(nestedObj);

        List<Field> nestedFields = Estimator.CLASS_NESTED_FIELDS.get(NestedObject.class);
        assertNotNull(nestedFields);
        assertEquals(1, nestedFields.size());
        assertEquals("inner", nestedFields.get(0).getName());
    }

    @Test
    void testNestedFieldsForObjectWithCollection() {
        // Test ObjectWithCollection: should contain "items"
        ObjectWithCollection obj = new ObjectWithCollection();
        obj.items.add("test");
        Estimator.estimate(obj);

        List<Field> fields = Estimator.CLASS_NESTED_FIELDS.get(ObjectWithCollection.class);
        assertNotNull(fields);
        assertEquals(1, fields.size());
        assertEquals("items", fields.get(0).getName());
    }

    @Test
    void testNestedFieldsExcludesIgnoredFields() {
        // Test ObjectWithIgnoredField: should only contain "tracked", not "ignored"
        ObjectWithIgnoredField obj = new ObjectWithIgnoredField();
        obj.tracked = "test";
        obj.ignored = new int[10];
        Estimator.estimate(obj);

        List<Field> fields = Estimator.CLASS_NESTED_FIELDS.get(ObjectWithIgnoredField.class);
        assertNotNull(fields);
        assertEquals(1, fields.size());
        assertEquals("tracked", fields.get(0).getName());
    }

    @Test
    void testNestedFieldsExcludesEnumFields() {
        // Test ObjectWithEnum: enum field should be excluded
        ObjectWithEnum obj = new ObjectWithEnum();
        obj.status = Status.ACTIVE;
        Estimator.estimate(obj);

        List<Field> fields = Estimator.CLASS_NESTED_FIELDS.get(ObjectWithEnum.class);
        assertNotNull(fields);
        // Enum field should be excluded, so the list should be empty
        assertTrue(fields.isEmpty());
    }

    @Test
    void testNestedFieldsExcludesStaticFields() {
        // Test ObjectWithStaticField: static field should be excluded
        ObjectWithStaticField.staticData = new int[10];
        ObjectWithStaticField obj = new ObjectWithStaticField();
        obj.instanceData = "test";
        Estimator.estimate(obj);

        List<Field> fields = Estimator.CLASS_NESTED_FIELDS.get(ObjectWithStaticField.class);
        assertNotNull(fields);
        assertEquals(1, fields.size());
        assertEquals("instanceData", fields.get(0).getName());
    }

    @Test
    void testNestedFieldsForComplexObject() {
        // Test a complex object with multiple field types
        ComplexObject obj = new ComplexObject();
        obj.stringField = "test";
        obj.listField = new ArrayList<>();
        obj.mapField = new HashMap<>();
        obj.nestedField = new SimpleObject(1, "nested");
        Estimator.estimate(obj);

        List<Field> fields = Estimator.CLASS_NESTED_FIELDS.get(ComplexObject.class);
        assertNotNull(fields);
        // Should contain: stringField, listField, mapField, nestedField
        // Should NOT contain: intField (primitive), enumField (enum)
        assertEquals(4, fields.size());

        Set<String> fieldNames = fields.stream()
                .map(Field::getName)
                .collect(java.util.stream.Collectors.toSet());
        assertTrue(fieldNames.contains("stringField"));
        assertTrue(fieldNames.contains("listField"));
        assertTrue(fieldNames.contains("mapField"));
        assertTrue(fieldNames.contains("nestedField"));
        assertFalse(fieldNames.contains("intField"));
        assertFalse(fieldNames.contains("enumField"));
    }

    @Test
    void testNestedFieldsForInheritedClass() {
        // Test SubClass: fields are cached per class in hierarchy
        SubClass obj = new SubClass();
        obj.baseField = "base";
        obj.subField = "sub";
        Estimator.estimate(obj);

        // SubClass's own fields
        List<Field> subFields = Estimator.CLASS_NESTED_FIELDS.get(SubClass.class);
        assertNotNull(subFields);
        assertEquals(1, subFields.size());
        assertEquals("subField", subFields.get(0).getName());

        // BaseClass's fields (cached separately)
        List<Field> baseFields = Estimator.CLASS_NESTED_FIELDS.get(BaseClass.class);
        assertNotNull(baseFields);
        assertEquals(1, baseFields.size());
        assertEquals("baseField", baseFields.get(0).getName());
    }

    @Test
    void testClassWithNoNestedFieldsIsCachedAsShallowMemory() {
        PrimitiveOnlyClass obj = new PrimitiveOnlyClass();
        obj.intValue = 42;
        obj.longValue = 100L;

        assertFalse(Estimator.SHALLOW_MEMORY_CLASSES.contains(PrimitiveOnlyClass.class));

        Estimator.estimate(obj);

        // Class with no reference fields should be auto-registered as shallow memory class
        assertTrue(Estimator.SHALLOW_MEMORY_CLASSES.contains(PrimitiveOnlyClass.class));
    }

    // ==================== Enum Field Tests ====================

    @Test
    void testEnumFieldsAreSkipped() {
        ObjectWithEnum obj = new ObjectWithEnum();
        obj.status = Status.ACTIVE;

        long size = Estimator.estimate(obj);
        // Enum fields should be skipped (they're singletons)
        // Size should be shallow size of the object only
        long shallow = Estimator.shallow(obj);
        assertEquals(shallow, size);
    }

    // ==================== Static Field Tests ====================

    @Test
    void testStaticFieldsAreSkipped() {
        ObjectWithStaticField.staticData = new int[1000];
        ObjectWithStaticField obj = new ObjectWithStaticField();
        obj.instanceData = "test";

        long size = Estimator.estimate(obj);
        // Static field should not be included
        long staticArraySize = Estimator.ARRAY_HEADER_SIZE + 1000 * 4;
        assertTrue(size < staticArraySize);
    }

    // ==================== Edge Case Tests ====================

    @Test
    void testEstimateCollectionWithAllNulls() {
        List<String> list = new ArrayList<>();
        list.add(null);
        list.add(null);
        list.add(null);

        long size = Estimator.estimate(list);
        assertEquals(Estimator.shallow(list), size);
    }

    @Test
    void testEstimateArrayWithAllNulls() {
        String[] array = new String[10];
        // All elements are null

        long size = Estimator.estimate(array);
        // Should be header + references only
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 10 * 4, size);
    }

    @Test
    void testEstimateSelfReferencingObject() {
        // This tests that depth limit prevents infinite recursion
        SelfReferencingClass obj = new SelfReferencingClass();
        obj.self = obj;

        // Should not hang or throw StackOverflowError
        long size = Estimator.estimate(obj);
        assertTrue(size > 0);
    }

    @Test
    void testEstimateLargeCollection() {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            list.add(i);
        }

        long size = Estimator.estimate(list);
        assertTrue(size > 0);
    }

    // ==================== Inheritance Tests ====================

    @Test
    void testEstimateSubclass() {
        SubClass obj = new SubClass();
        obj.baseField = "base";
        obj.subField = "sub";

        long size = Estimator.estimate(obj);
        // Should include fields from both base and subclass
        assertTrue(size > Estimator.shallow(obj));
    }

    @Test
    void testIgnoreMemoryTrackOnSuperclass() {
        SubClassOfIgnored obj = new SubClassOfIgnored();
        obj.subData = "data";

        // When any class in the hierarchy has @IgnoreMemoryTrack, the entire object returns 0
        // This prevents partial memory tracking which could be misleading
        long size = Estimator.estimate(obj);
        assertEquals(0, size);
    }

    @Test
    void testIgnoreMemoryTrackOnMiddleClass() {
        // Test class hierarchy: SubSubClass -> IgnoredMiddleClass (@IgnoreMemoryTrack) -> BaseClass
        SubSubClassOfIgnoredMiddle obj = new SubSubClassOfIgnoredMiddle();
        obj.baseField = "base";
        obj.subSubField = "subsub";

        // Should return 0 because IgnoredMiddleClass in the hierarchy has @IgnoreMemoryTrack
        long size = Estimator.estimate(obj);
        assertEquals(0, size);
    }

    @Test
    void testNoIgnoreMemoryTrackInHierarchy() {
        // Test class hierarchy without @IgnoreMemoryTrack: SubClass -> BaseClass
        SubClass obj = new SubClass();
        obj.baseField = "base";
        obj.subField = "sub";

        // Should estimate normally since no class in hierarchy has @IgnoreMemoryTrack
        long size = Estimator.estimate(obj);
        assertTrue(size > 0);
        assertTrue(size > Estimator.shallow(obj));
    }

    // ==================== Helper Classes ====================

    static class SimpleObject {
        int id;
        String name;

        SimpleObject(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    static class NestedObject {
        SimpleObject inner;
    }

    static class ObjectWithCollection {
        List<String> items = new ArrayList<>();
    }

    static class DeeplyNestedObject {
        int level;
        DeeplyNestedObject next;
    }

    private DeeplyNestedObject createDeeplyNestedObject(int depth) {
        DeeplyNestedObject root = new DeeplyNestedObject();
        root.level = 0;
        DeeplyNestedObject current = root;
        for (int i = 1; i < depth; i++) {
            current.next = new DeeplyNestedObject();
            current.next.level = i;
            current = current.next;
        }
        return root;
    }

    @IgnoreMemoryTrack
    static class IgnoredClass {
        int[] data;
    }

    static class ObjectWithIgnoredField {
        String tracked;
        @IgnoreMemoryTrack
        int[] ignored;
    }

    @ShallowMemory
    static class ShallowOnlyClass {
        List<String> data;
    }

    static class CustomEstimatedClass {
        int data;
    }

    static class PrimitiveOnlyClass {
        int intValue;
        long longValue;
        double doubleValue;
        boolean boolValue;
    }

    enum Status {
        ACTIVE, INACTIVE
    }

    static class ObjectWithEnum {
        Status status;
    }

    static class ObjectWithStaticField {
        static int[] staticData;
        String instanceData;
    }

    static class SelfReferencingClass {
        SelfReferencingClass self;
    }

    static class BaseClass {
        String baseField;
    }

    static class SubClass extends BaseClass {
        String subField;
    }

    @IgnoreMemoryTrack
    static class IgnoredBaseClass {
        String ignoredData;
    }

    static class SubClassOfIgnored extends IgnoredBaseClass {
        String subData;
    }

    @IgnoreMemoryTrack
    static class IgnoredMiddleClass extends BaseClass {
        String middleData;
    }

    static class SubSubClassOfIgnoredMiddle extends IgnoredMiddleClass {
        String subSubField;
    }

    static class ComplexObject {
        int intField;
        String stringField;
        List<String> listField;
        Map<String, Integer> mapField;
        SimpleObject nestedField;
        Status enumField;
    }

    // ==================== Hidden Class Tests ====================

    @Test
    void testShallowSizeLambdaExpression() {
        // Lambda expressions generate hidden classes (or synthetic classes with $$Lambda$ in name)
        Supplier<String> lambda = () -> "hello";

        long size = Estimator.shallow(lambda);
        // Hidden classes should return HIDDEN_CLASS_SHALLOW_SIZE (16)
        assertEquals(16, size);
    }

    @Test
    void testShallowSizeLambdaWithCapture() {
        // Lambda that captures a variable
        String captured = "captured value";
        Function<String, String> lambda = (s) -> captured + s;

        long size = Estimator.shallow(lambda);
        // Hidden classes should return HIDDEN_CLASS_SHALLOW_SIZE (16)
        assertEquals(16, size);
    }

    @Test
    void testEstimateLambdaExpression() {
        Supplier<Integer> lambda = () -> 42;

        long size = Estimator.estimate(lambda);
        assertEquals(16, size);
    }

    @Test
    void testEstimateObjectContainingLambda() {
        ObjectWithLambda obj = new ObjectWithLambda();
        obj.name = "test";
        obj.callback = () -> "callback result";

        long size = Estimator.estimate(obj);
        // Size should include object shallow size + string size + lambda size (16)
        assertTrue(size > Estimator.shallow(obj));
    }

    @Test
    void testEstimateCollectionOfLambdas() {
        List<Supplier<String>> lambdas = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final int index = i;
            lambdas.add(() -> "value" + index);
        }

        long size = Estimator.estimate(lambdas);
        // Size should be positive and include list overhead + lambda sizes
        assertTrue(size > Estimator.shallow(lambdas));
    }

    @Test
    void testShallowSizeMethodReference() {
        // Method references also generate hidden/synthetic classes
        Function<String, Integer> methodRef = String::length;

        long size = Estimator.shallow(methodRef);
        // Method references should also return HIDDEN_CLASS_SHALLOW_SIZE (16)
        assertEquals(16, size);
    }

    @Test
    void testLambdaClassIsCachedCorrectly() {
        Supplier<String> lambda = () -> "test";
        Class<?> lambdaClass = lambda.getClass();

        // First call should populate the cache
        long size1 = Estimator.shallow(lambda);
        assertTrue(Estimator.SHALLOW_SIZE_CACHE.containsKey(lambdaClass));

        // Second call should use cached value
        long size2 = Estimator.shallow(lambda);
        assertEquals(size1, size2);
        assertEquals(16, size1);
    }

    @Test
    void testMultipleDifferentLambdasHaveSameShallowSize() {
        Supplier<String> lambda1 = () -> "first";
        Supplier<String> lambda2 = () -> "second";
        Function<Integer, String> lambda3 = (i) -> "number: " + i;

        long size1 = Estimator.shallow(lambda1);
        long size2 = Estimator.shallow(lambda2);
        long size3 = Estimator.shallow(lambda3);

        // All hidden classes should have the same shallow size
        assertEquals(16, size1);
        assertEquals(16, size2);
        assertEquals(16, size3);
    }

    // Helper class for hidden class tests
    static class ObjectWithLambda {
        String name;
        Supplier<String> callback;
    }
}
