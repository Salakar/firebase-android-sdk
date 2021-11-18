// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.firebase.crashlytics.internal.metadata;

import com.google.firebase.crashlytics.internal.CrashlyticsTestCase;
import com.google.firebase.crashlytics.internal.persistence.FileStore;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MetaDataStoreTest extends CrashlyticsTestCase {

  private static final String SESSION_ID_1 = "session1";
  private static final String SESSION_ID_2 = "session2";

  private static final String USER_ID = "testUserId";

  private static final String KEY_1 = "testKey1";
  private static final String KEY_2 = "testKey2";
  private static final String KEY_3 = "testKey3";
  private static final String VALUE_1 = "testValue1";
  private static final String VALUE_2 = "testValue2";
  private static final String VALUE_3 = "testValue3";

  private static final String UNICODE = "あいうえおかきくけ";

  private static final String ESCAPED = "\ttest\nvalue";

  private FileStore fileStore;

  private MetaDataStore storeUnderTest;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    fileStore = new FileStore(getContext());
    storeUnderTest = new MetaDataStore(fileStore);
  }

  private UserMetadata metadataWithUserId() {
    return metadataWithUserId(USER_ID);
  }

  private UserMetadata metadataWithUserId(String id) {
    UserMetadata metadata = new UserMetadata(fileStore);
    metadata.setUserId(id);
    return metadata;
  }

  public void testWriteUserData_allFields() {
    storeUnderTest.writeUserData(SESSION_ID_1, metadataWithUserId().getUserId());
    UserMetadata userData = UserMetadata.loadFromExistingSession(SESSION_ID_1, fileStore);
    assertEquals(USER_ID, userData.getUserId());
  }

  public void testWriteUserData_noFields() {
    storeUnderTest.writeUserData(SESSION_ID_1, new UserMetadata(fileStore).getUserId());
    UserMetadata userData = UserMetadata.loadFromExistingSession(SESSION_ID_1, fileStore);
    assertNull(userData.getUserId());
  }

  public void testWriteUserData_singleField() {
    storeUnderTest.writeUserData(SESSION_ID_1, metadataWithUserId().getUserId());
    UserMetadata userData = UserMetadata.loadFromExistingSession(SESSION_ID_1, fileStore);
    assertEquals(USER_ID, userData.getUserId());
  }

  public void testWriteUserData_null() {
    storeUnderTest.writeUserData(SESSION_ID_1, metadataWithUserId(null).getUserId());
    UserMetadata userData = UserMetadata.loadFromExistingSession(SESSION_ID_1, fileStore);
    assertEquals(null, userData.getUserId());
  }

  public void testWriteUserData_emptyString() {
    storeUnderTest.writeUserData(SESSION_ID_1, metadataWithUserId("").getUserId());
    UserMetadata userData = UserMetadata.loadFromExistingSession(SESSION_ID_1, fileStore);
    assertEquals("", userData.getUserId());
  }

  public void testWriteUserData_unicode() {
    storeUnderTest.writeUserData(SESSION_ID_1, metadataWithUserId(UNICODE).getUserId());
    UserMetadata userData = UserMetadata.loadFromExistingSession(SESSION_ID_1, fileStore);
    assertEquals(UNICODE, userData.getUserId());
  }

  public void testWriteUserData_escaped() {
    storeUnderTest.writeUserData(SESSION_ID_1, metadataWithUserId(ESCAPED).getUserId());
    UserMetadata userData = UserMetadata.loadFromExistingSession(SESSION_ID_1, fileStore);
    assertEquals(ESCAPED.trim(), userData.getUserId());
  }

  public void testWriteUserData_readDifferentSession() {
    storeUnderTest.writeUserData(SESSION_ID_1, metadataWithUserId().getUserId());
    UserMetadata userData = UserMetadata.loadFromExistingSession(SESSION_ID_2, fileStore);
    assertNull(userData.getUserId());
  }

  public void testReadUserData_noStoredData() {
    UserMetadata userData = UserMetadata.loadFromExistingSession(SESSION_ID_1, fileStore);
    assertNull(userData.getUserId());
  }

  // Keys

  public void testWriteKeys() {
    final Map<String, String> keys =
        new HashMap<String, String>() {
          {
            put(KEY_1, VALUE_1);
            put(KEY_2, VALUE_2);
            put(KEY_3, VALUE_3);
          }
        };
    storeUnderTest.writeKeyData(SESSION_ID_1, keys);
    final Map<String, String> readKeys = storeUnderTest.readKeyData(SESSION_ID_1);
    assertEqualMaps(keys, readKeys);
  }

  public void testWriteKeys_noValues() {
    final Map<String, String> keys = Collections.emptyMap();
    storeUnderTest.writeKeyData(SESSION_ID_1, keys);
    final Map<String, String> readKeys = storeUnderTest.readKeyData(SESSION_ID_1);
    assertEqualMaps(keys, readKeys);
  }

  public void testWriteKeys_nullValues() {
    final Map<String, String> keys =
        new HashMap<String, String>() {
          {
            put(KEY_1, null);
            put(KEY_2, VALUE_2);
            put(KEY_3, null);
          }
        };
    storeUnderTest.writeKeyData(SESSION_ID_1, keys);
    final Map<String, String> readKeys = storeUnderTest.readKeyData(SESSION_ID_1);
    assertEqualMaps(keys, readKeys);
  }

  public void testWriteKeys_emptyStrings() {
    final Map<String, String> keys =
        new HashMap<String, String>() {
          {
            put(KEY_1, "");
            put(KEY_2, VALUE_2);
            put(KEY_3, "");
          }
        };
    storeUnderTest.writeKeyData(SESSION_ID_1, keys);
    final Map<String, String> readKeys = storeUnderTest.readKeyData(SESSION_ID_1);
    assertEqualMaps(keys, readKeys);
  }

  public void testWriteKeys_unicode() {
    final Map<String, String> keys =
        new HashMap<String, String>() {
          {
            put(KEY_1, UNICODE);
            put(KEY_2, VALUE_2);
            put(KEY_3, UNICODE);
          }
        };
    storeUnderTest.writeKeyData(SESSION_ID_1, keys);
    final Map<String, String> readKeys = storeUnderTest.readKeyData(SESSION_ID_1);
    assertEqualMaps(keys, readKeys);
  }

  public void testWriteKeys_escaped() {
    final Map<String, String> keys =
        new HashMap<String, String>() {
          {
            put(KEY_1, ESCAPED);
            put(KEY_2, VALUE_2);
            put(KEY_3, ESCAPED);
          }
        };
    storeUnderTest.writeKeyData(SESSION_ID_1, keys);
    final Map<String, String> readKeys = storeUnderTest.readKeyData(SESSION_ID_1);
    assertEqualMaps(keys, readKeys);
  }

  public void testWriteKeys_readDifferentSession() {
    final Map<String, String> keys =
        new HashMap<String, String>() {
          {
            put(KEY_1, VALUE_2);
            put(KEY_2, VALUE_2);
            put(KEY_3, VALUE_3);
          }
        };
    storeUnderTest.writeKeyData(SESSION_ID_1, keys);
    final Map<String, String> readKeys = storeUnderTest.readKeyData(SESSION_ID_2);
    assertEquals(0, readKeys.size());
  }

  // Ensures the Internal Keys and User Custom Keys are stored separately
  public void testWriteKeys_readSeparateFromUser() {
    final Map<String, String> keys =
        new HashMap<String, String>() {
          {
            put(KEY_1, VALUE_1);
          }
        };

    final Map<String, String> internalKeys =
        new HashMap<String, String>() {
          {
            put(KEY_2, VALUE_2);
            put(KEY_3, VALUE_3);
          }
        };

    storeUnderTest.writeKeyData(SESSION_ID_1, keys);
    storeUnderTest.writeKeyData(SESSION_ID_1, internalKeys, /*isInternal=*/ true);

    final Map<String, String> readKeys = storeUnderTest.readKeyData(SESSION_ID_1);
    final Map<String, String> readInternalKeys = storeUnderTest.readKeyData(SESSION_ID_1, true);

    assertEqualMaps(keys, readKeys);
    assertEqualMaps(internalKeys, readInternalKeys);
  }

  public void testReadKeys_noStoredData() {
    final Map<String, String> readKeys = storeUnderTest.readKeyData(SESSION_ID_1);
    assertEquals(0, readKeys.size());
  }

  public static void assertEqualMaps(Map<String, String> expected, Map<String, String> actual) {
    assertEquals(expected.size(), actual.size());
    for (String key : expected.keySet()) {
      assertTrue(actual.containsKey(key));
      assertEquals(expected.get(key), actual.get(key));
    }
  }
}
