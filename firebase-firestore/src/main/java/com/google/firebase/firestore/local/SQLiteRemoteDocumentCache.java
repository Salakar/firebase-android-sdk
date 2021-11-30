// Copyright 2018 Google LLC
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

package com.google.firebase.firestore.local;

import static com.google.firebase.firestore.util.Assert.fail;
import static com.google.firebase.firestore.util.Assert.hardAssert;
import static com.google.firebase.firestore.util.Util.repeatSequence;

import androidx.annotation.VisibleForTesting;
import com.google.firebase.Timestamp;
import com.google.firebase.database.collection.ImmutableSortedMap;
import com.google.firebase.firestore.core.Query;
import com.google.firebase.firestore.model.DocumentCollections;
import com.google.firebase.firestore.model.DocumentKey;
import com.google.firebase.firestore.model.FieldIndex.IndexOffset;
import com.google.firebase.firestore.model.MutableDocument;
import com.google.firebase.firestore.model.ResourcePath;
import com.google.firebase.firestore.model.SnapshotVersion;
import com.google.firebase.firestore.util.BackgroundQueue;
import com.google.firebase.firestore.util.Executors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

final class SQLiteRemoteDocumentCache implements RemoteDocumentCache {
  /** The number of bind args per collection group in {@link #getAll(String, IndexOffset, int)} */
  @VisibleForTesting static final int GET_ALL_BINDS_PER_STATEMENT = 8;

  private final SQLitePersistence db;
  private final LocalSerializer serializer;
  private IndexManager indexManager;

  SQLiteRemoteDocumentCache(SQLitePersistence persistence, LocalSerializer serializer) {
    this.db = persistence;
    this.serializer = serializer;
  }

  @Override
  public void setIndexManager(IndexManager indexManager) {
    this.indexManager = indexManager;
  }

  @Override
  public void add(MutableDocument document, SnapshotVersion readTime) {
    hardAssert(
        !readTime.equals(SnapshotVersion.NONE),
        "Cannot add document to the RemoteDocumentCache with a read time of zero");

    String path = pathForKey(document.getKey());
    Timestamp timestamp = readTime.getTimestamp();
    MessageLite message = serializer.encodeMaybeDocument(document);

    db.execute(
        "INSERT OR REPLACE INTO remote_documents "
            + "(path, read_time_seconds, read_time_nanos, contents) "
            + "VALUES (?, ?, ?, ?)",
        path,
        timestamp.getSeconds(),
        timestamp.getNanoseconds(),
        message.toByteArray());

    indexManager.addToCollectionParentIndex(document.getKey().getPath().popLast());
  }

  @Override
  public void remove(DocumentKey documentKey) {
    String path = pathForKey(documentKey);

    db.execute("DELETE FROM remote_documents WHERE path = ?", path);
  }

  @Override
  public MutableDocument get(DocumentKey documentKey) {
    String path = pathForKey(documentKey);

    MutableDocument document =
        db.query(
                "SELECT contents, read_time_seconds, read_time_nanos "
                    + "FROM remote_documents WHERE path = ?")
            .binding(path)
            .firstValue(row -> decodeMaybeDocument(row.getBlob(0), row.getInt(1), row.getInt(2)));
    return document != null ? document : MutableDocument.newInvalidDocument(documentKey);
  }

  @Override
  public Map<DocumentKey, MutableDocument> getAll(Iterable<DocumentKey> documentKeys) {
    List<Object> args = new ArrayList<>();
    for (DocumentKey key : documentKeys) {
      args.add(EncodedPath.encode(key.getPath()));
    }

    Map<DocumentKey, MutableDocument> results = new HashMap<>();
    for (DocumentKey key : documentKeys) {
      // Make sure each key has a corresponding entry, which is null in case the document is not
      // found.
      results.put(key, MutableDocument.newInvalidDocument(key));
    }

    SQLitePersistence.LongQuery longQuery =
        new SQLitePersistence.LongQuery(
            db,
            "SELECT contents, read_time_seconds, read_time_nanos FROM remote_documents "
                + "WHERE path IN (",
            args,
            ") ORDER BY path");

    while (longQuery.hasMoreSubqueries()) {
      longQuery
          .performNextSubquery()
          .forEach(
              row -> {
                MutableDocument decoded =
                    decodeMaybeDocument(row.getBlob(0), row.getInt(1), row.getInt(2));
                results.put(decoded.getKey(), decoded);
              });
    }

    return results;
  }

  @Override
  public Map<DocumentKey, MutableDocument> getAll(
      String collectionGroup, IndexOffset offset, int count) {
    List<ResourcePath> collectionParents = indexManager.getCollectionParents(collectionGroup);
    List<ResourcePath> collections = new ArrayList<>(collectionParents.size());
    for (ResourcePath collectionParent : collectionParents) {
      collections.add(collectionParent.append(collectionGroup));
    }

    if (collections.isEmpty()) {
      return Collections.emptyMap();
    } else if (GET_ALL_BINDS_PER_STATEMENT * collections.size() < SQLitePersistence.MAX_ARGS) {
      return getAll(collections, offset, count);
    } else {
      // We need to fan out our collection scan since SQLite only supports 999 binds per statement.
      Map<DocumentKey, MutableDocument> results = new HashMap<>();
      int pageSize = SQLitePersistence.MAX_ARGS / GET_ALL_BINDS_PER_STATEMENT;
      for (int i = 0; i < collections.size(); i += pageSize) {
        results.putAll(
            getAll(
                collections.subList(i, Math.min(collections.size(), i + pageSize)), offset, count));
      }
      return results;
    }
  }

  /**
   * Returns the next {@code count} documents from the provided collections, ordered by read time.
   */
  private Map<DocumentKey, MutableDocument> getAll(
      List<ResourcePath> collections, IndexOffset offset, int count) {
    Timestamp readTime = offset.getReadTime().getTimestamp();
    DocumentKey documentKey = offset.getDocumentKey();

    // TODO(indexing): Add path_length

    StringBuilder sql =
        repeatSequence(
            "SELECT contents, read_time_seconds, read_time_nanos, path "
                + "FROM remote_documents "
                + "WHERE path >= ? AND path < ? "
                + "AND (read_time_seconds > ? OR ( "
                + "read_time_seconds = ? AND read_time_nanos > ?) OR ( "
                + "read_time_seconds = ? AND read_time_nanos = ? and path > ?)) ",
            collections.size(),
            " UNION ");
    sql.append("ORDER BY read_time_seconds, read_time_nanos, path LIMIT ?");

    Object[] bindVars = new Object[GET_ALL_BINDS_PER_STATEMENT * collections.size() + 1];
    int i = 0;
    for (ResourcePath collectionParent : collections) {
      String prefixPath = EncodedPath.encode(collectionParent);
      bindVars[i++] = prefixPath;
      bindVars[i++] = EncodedPath.prefixSuccessor(prefixPath);
      bindVars[i++] = readTime.getSeconds();
      bindVars[i++] = readTime.getSeconds();
      bindVars[i++] = readTime.getNanoseconds();
      bindVars[i++] = readTime.getSeconds();
      bindVars[i++] = readTime.getNanoseconds();
      bindVars[i++] = EncodedPath.encode(documentKey.getPath());
    }
    bindVars[i] = count;

    BackgroundQueue backgroundQueue = new BackgroundQueue();
    Map<DocumentKey, MutableDocument>[] results =
        (HashMap<DocumentKey, MutableDocument>[]) (new HashMap[] {new HashMap()});

    db.query(sql.toString())
        .binding(bindVars)
        .forEach(
            row -> {
              final byte[] rawDocument = row.getBlob(0);
              final int[] readTimeSeconds = {row.getInt(1)};
              final int[] readTimeNanos = {row.getInt(2)};

              Executor executor = row.isLast() ? Executors.DIRECT_EXECUTOR : backgroundQueue;
              executor.execute(
                  () -> {
                    MutableDocument document =
                        decodeMaybeDocument(rawDocument, readTimeSeconds[0], readTimeNanos[0]);
                    synchronized (SQLiteRemoteDocumentCache.this) {
                      results[0].put(document.getKey(), document);
                    }
                  });
            });

    try {
      backgroundQueue.drain();
    } catch (InterruptedException e) {
      fail("Interrupted while deserializing documents", e);
    }

    return results[0];
  }

  @Override
  public ImmutableSortedMap<DocumentKey, MutableDocument> getAllDocumentsMatchingQuery(
      final Query query, IndexOffset offset) {
    hardAssert(
        !query.isCollectionGroupQuery(),
        "CollectionGroup queries should be handled in LocalDocumentsView");

    // Use the query path as a prefix for testing if a document matches the query.
    ResourcePath prefix = query.getPath();
    int immediateChildrenPathLength = prefix.length() + 1;

    String prefixPath = EncodedPath.encode(prefix);
    String prefixSuccessorPath = EncodedPath.prefixSuccessor(prefixPath);

    BackgroundQueue backgroundQueue = new BackgroundQueue();

    ImmutableSortedMap<DocumentKey, MutableDocument>[] matchingDocuments =
        (ImmutableSortedMap<DocumentKey, MutableDocument>[])
            new ImmutableSortedMap[] {DocumentCollections.emptyMutableDocumentMap()};

    SQLitePersistence.Query sqlQuery;
    if (IndexOffset.NONE.equals(offset)) {
      sqlQuery =
          db.query(
                  "SELECT path, contents, read_time_seconds, read_time_nanos "
                      + "FROM remote_documents WHERE path >= ? AND path < ?")
              .binding(prefixPath, prefixSuccessorPath);
    } else {
      Timestamp readTime = offset.getReadTime().getTimestamp();
      DocumentKey documentKey = offset.getDocumentKey();

      sqlQuery =
          db.query(
                  "SELECT path, contents, read_time_seconds, read_time_nanos "
                      + "FROM remote_documents WHERE path >= ? AND path < ? AND ("
                      + "read_time_seconds > ? OR ("
                      + "read_time_seconds = ? AND read_time_nanos > ?) OR ("
                      + "read_time_seconds = ? AND read_time_nanos = ? and path > ?))")
              .binding(
                  prefixPath,
                  prefixSuccessorPath,
                  readTime.getSeconds(),
                  readTime.getSeconds(),
                  readTime.getNanoseconds(),
                  readTime.getSeconds(),
                  readTime.getNanoseconds(),
                  EncodedPath.encode(documentKey.getPath()));
    }
    sqlQuery.forEach(
        row -> {
          // TODO: Actually implement a single-collection query
          //
          // The query is actually returning any path that starts with the query path prefix
          // which may include documents in subcollections. For example, a query on 'rooms'
          // will return rooms/abc/messages/xyx but we shouldn't match it. Fix this by
          // discarding rows with document keys more than one segment longer than the query
          // path.
          ResourcePath path = EncodedPath.decodeResourcePath(row.getString(0));
          if (path.length() != immediateChildrenPathLength) {
            return;
          }

          // Store row values in array entries to provide the correct context inside the executor.
          final byte[] rawDocument = row.getBlob(1);
          final int[] readTimeSeconds = {row.getInt(2)};
          final int[] readTimeNanos = {row.getInt(3)};

          // Since scheduling background tasks incurs overhead, we only dispatch to a
          // background thread if there are still some documents remaining.
          Executor executor = row.isLast() ? Executors.DIRECT_EXECUTOR : backgroundQueue;
          executor.execute(
              () -> {
                MutableDocument document =
                    decodeMaybeDocument(rawDocument, readTimeSeconds[0], readTimeNanos[0]);
                if (document.isFoundDocument() && query.matches(document)) {
                  synchronized (SQLiteRemoteDocumentCache.this) {
                    matchingDocuments[0] = matchingDocuments[0].insert(document.getKey(), document);
                  }
                }
              });
        });

    try {
      backgroundQueue.drain();
    } catch (InterruptedException e) {
      fail("Interrupted while deserializing documents", e);
    }

    return matchingDocuments[0];
  }

  @Override
  public SnapshotVersion getLatestReadTime() {
    SnapshotVersion latestReadTime =
        db.query(
                "SELECT read_time_seconds, read_time_nanos "
                    + "FROM remote_documents ORDER BY read_time_seconds DESC, read_time_nanos DESC "
                    + "LIMIT 1")
            .firstValue(row -> new SnapshotVersion(new Timestamp(row.getLong(0), row.getInt(1))));
    return latestReadTime != null ? latestReadTime : SnapshotVersion.NONE;
  }

  private String pathForKey(DocumentKey key) {
    return EncodedPath.encode(key.getPath());
  }

  private MutableDocument decodeMaybeDocument(
      byte[] bytes, int readTimeSeconds, int readTimeNanos) {
    try {
      return serializer
          .decodeMaybeDocument(com.google.firebase.firestore.proto.MaybeDocument.parseFrom(bytes))
          .withReadTime(new SnapshotVersion(new Timestamp(readTimeSeconds, readTimeNanos)));
    } catch (InvalidProtocolBufferException e) {
      throw fail("MaybeDocument failed to parse: %s", e);
    }
  }
}
