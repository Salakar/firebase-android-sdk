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

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.annotation.VisibleForTesting;
import com.google.firebase.crashlytics.internal.persistence.FileStore;
import java.util.Map;

/** Handles attributes set by the user. */
public class UserMetadata {

  public static final String USERDATA_FILENAME = "user-data";
  public static final String KEYDATA_FILENAME = "keys";
  public static final String INTERNAL_KEYDATA_FILENAME = "internal-keys";

  @VisibleForTesting public static final int MAX_ATTRIBUTES = 64;
  @VisibleForTesting public static final int MAX_ATTRIBUTE_SIZE = 1024;
  @VisibleForTesting public static final int MAX_INTERNAL_KEY_SIZE = 8192;

  private final MetaDataStore metaDataStore;
  private String userId = null;
  private final KeysMap customKeys = new KeysMap(MAX_ATTRIBUTES, MAX_ATTRIBUTE_SIZE);
  private final KeysMap internalKeys = new KeysMap(MAX_ATTRIBUTES, MAX_INTERNAL_KEY_SIZE);

  public static UserMetadata loadFromExistingSession(String sessionId, FileStore fileStore) {
    MetaDataStore store = new MetaDataStore(fileStore);
    UserMetadata metadata = new UserMetadata(fileStore);
    metadata.customKeys.setKeys(store.readKeyData(sessionId, false));
    metadata.internalKeys.setKeys(store.readKeyData(sessionId, true));
    metadata.setUserId(store.readUserId(sessionId));

    return metadata;
  }

  public UserMetadata(FileStore fileStore) {
    this.metaDataStore = new MetaDataStore(fileStore);
  }

  @Nullable
  public String getUserId() {
    return userId;
  }

  public void setUserId(String identifier) {
    userId = customKeys.sanitizeAttribute(identifier);
  }

  @NonNull
  public Map<String, String> getCustomKeys() {
    return customKeys.getKeys();
  }

  /** @return true when key is new, or the value associated with key has changed */
  public boolean setCustomKey(String key, String value) {
    return customKeys.setKey(key, value);
  }

  public void setCustomKeys(Map<String, String> keysAndValues) {
    customKeys.setKeys(keysAndValues);
  }

  public Map<String, String> getInternalKeys() {
    return internalKeys.getKeys();
  }

  public boolean setInternalKey(String key, String value) {
    return internalKeys.setKey(key, value);
  }

  public void serializeKeysIfNeeded(String sessionId, boolean isInternal) {
    if (isInternal) {
      serializeInternalKeysIfNeeded(sessionId);
    } else {
      serializeCustomKeysIfNeeded(sessionId);
    }
  }

  public void serializeCustomKeysIfNeeded(String sessionId) {
    metaDataStore.writeKeyData(sessionId, getCustomKeys(), false);
  }

  public void serializeInternalKeysIfNeeded(String sessionId) {
    metaDataStore.writeKeyData(sessionId, getInternalKeys(), true);
  }

  public void serializeUserDataIfNeeded(String sessionId) {
    metaDataStore.writeUserData(sessionId, userId);
  }
}
