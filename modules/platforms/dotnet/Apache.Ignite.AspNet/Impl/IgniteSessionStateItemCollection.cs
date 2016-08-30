﻿/*
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

namespace Apache.Ignite.AspNet.Impl
{
    using System;
    using System.Collections;
    using System.Collections.Specialized;
    using System.Diagnostics;
    using System.Web.SessionState;
    using Apache.Ignite.Core.Impl.Collections;

    /// <summary>
    /// Wrapper for <see cref="KeyValueDirtyTrackedCollection" />.
    /// </summary>
    internal class IgniteSessionStateItemCollection : ISessionStateItemCollection
    {
        // TODO: Dedicated test!

        private readonly KeyValueDirtyTrackedCollection _collection;

        public IgniteSessionStateItemCollection(KeyValueDirtyTrackedCollection collection)
        {
            Debug.Assert(collection != null);

            _collection = collection;
        }

        public IEnumerator GetEnumerator()
        {
            return _collection.GetEnumerator();
        }

        public void CopyTo(Array array, int index)
        {
            throw new NotImplementedException();
        }

        public int Count
        {
            get { return _collection.Count; }
        }

        public object SyncRoot
        {
            get { return _collection; }
        }

        public bool IsSynchronized
        {
            get { return false; }
        }

        public void Remove(string name)
        {
            _collection.Remove(name);
        }

        public void RemoveAt(int index)
        {
            _collection.RemoveAt(index);
        }

        public void Clear()
        {
            _collection.Clear();
        }

        object ISessionStateItemCollection.this[string name]
        {
            get { return _collection[name]; }
            set { _collection[name] = value; }
        }

        object ISessionStateItemCollection.this[int index]
        {
            get { return _collection[index]; }
            set { _collection[index] = value; }
        }

        public NameObjectCollectionBase.KeysCollection Keys
        {
            get
            {
                // TODO: Convert somehow? 
                //return _collection.Keys;
                return null;
            }
        }

        public bool Dirty
        {
            get { return _collection.IsDirty; }
            set { _collection.IsDirty = value; }
        }
    }
}