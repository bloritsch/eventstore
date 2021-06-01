/*
 * Copyright (c) 2021.  D-Haven.org
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
 */

package eventstore

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/oklog/ulid/v2"
	"reflect"
	"time"
)

type BadgerEventStore struct {
	RootDir                    string
	MemoryOnly                 bool
	EncryptionKey              []byte
	EncryptionRotationDuration time.Duration
	db                         *badger.DB
	typeRegistery              map[string]reflect.Type
}

type Record struct {
	Id        ulid.ULID
	Timestamp time.Time
	Type      string
	Content   []byte
}

func MemoryStore() EventStore {
	return &BadgerEventStore{
		MemoryOnly:    true,
		RootDir:       "",
		typeRegistery: make(map[string]reflect.Type),
	}
}

func FileStore(path string, key []byte, rotationDur time.Duration) EventStore {
	return &BadgerEventStore{
		RootDir:                    path,
		MemoryOnly:                 false,
		EncryptionKey:              key,
		EncryptionRotationDuration: rotationDur,
		typeRegistery:              make(map[string]reflect.Type),
	}
}

func (b *BadgerEventStore) kvstore() (*badger.DB, error) {
	if b.db != nil {
		return b.db, nil
	}

	opts := badger.DefaultOptions(b.RootDir).WithInMemory(b.MemoryOnly)

	if b.EncryptionKey != nil && len(b.EncryptionKey) >= 128 {
		opts = opts.WithEncryptionKey(b.EncryptionKey)
		opts = opts.WithEncryptionKeyRotationDuration(b.EncryptionRotationDuration)
		// May need to tune this.. data store shouldn't get too big
		opts = opts.WithIndexCacheSize(100 << 20) // 100 mb
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	b.db = db
	return b.db, nil
}

func (b *BadgerEventStore) Append(aggregate string, content interface{}) error {
	now := time.Now().UTC()

	record := Record{
		Id:        NewId(now),
		Timestamp: now,
		Type:      typeName(content),
	}

	var c bytes.Buffer
	enc := gob.NewEncoder(&c)
	if err := enc.Encode(&content); err != nil {
		return err
	}

	record.Content = c.Bytes()

	k, err := record.Id.MarshalText()
	if err != nil {
		return err
	}

	key := []byte(fmt.Sprintf("%s:%s", aggregate, k))
	value, err := json.Marshal(record)
	if err != nil {
		return err
	}

	db, err := b.kvstore()
	if err != nil {
		return err
	}

	if err = db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	}); err != nil {
		return err
	}

	// FIXME: This shouldn't be necessary, but writes in rapid succession can fail otherwise. (i.e. in unit tests)
	// time.Sleep(1 * time.Millisecond)

	return nil
}

func (b *BadgerEventStore) Register(t interface{}) {
	gob.Register(t)
	name := typeName(t)
	b.typeRegistery[name] = reflect.TypeOf(t)
}

func typeName(t interface{}) string {
	return fmt.Sprintf("%T", t)
}

func (b *BadgerEventStore) makeInstance(name string) interface{} {
	return reflect.New(b.typeRegistery[name]).Elem().Interface()
}

func (b *BadgerEventStore) Read(aggregate string) ([]interface{}, error) {
	db, err := b.kvstore()

	if err != nil {
		return nil, err
	}

	prefix := []byte(aggregate + ":")
	var values []interface{}

	if err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		// Walk all the events using the aggregate as a prefix
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var record Record
				if err = json.Unmarshal(val, &record); err != nil {
					return err
				}

				c := bytes.NewReader(record.Content)
				dec := gob.NewDecoder(c)
				v := b.makeInstance(record.Type)
				if err = dec.Decode(&v); err != nil {
					return err
				}

				values = append(values, v)
				return nil
			})

			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return values, nil
}

func (b *BadgerEventStore) ListKeys() ([]string, error) {
	var keys []string
	db, err := b.kvstore()

	if err != nil {
		return nil, err
	}

	if err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			keys = append(keys, string(item.Key()))
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return keys, nil
}

func (b *BadgerEventStore) ListKeysForAggregate(aggregate string) ([]string, error) {
	prefix := []byte(aggregate + ":")
	var keys []string
	db, err := b.kvstore()

	if err != nil {
		return nil, err
	}

	if err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			keys = append(keys, string(item.Key()))
		}

		time.Sleep(1 * time.Millisecond)
		return nil
	}); err != nil {
		return nil, err
	}

	return keys, nil
}

func (b *BadgerEventStore) Close() error {
	if b.db != nil {
		if err := b.db.Close(); err != nil {
			return err
		}
		b.db = nil
	}

	return nil
}
