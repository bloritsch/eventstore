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
	"fmt"
	"reflect"
	"testing"
)

func TestBadgerEventStore_Append(t *testing.T) {
	store := MemoryStore()
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	store.Register(Test{})
	fact := "test.1"
	content := Test{Value: 1}

	err := store.Append(fact, content)
	if err != nil {
		t.Error(err)
	}

	results, err := store.Read(fact)
	if err != nil {
		t.Error(err)
		return
	}

	if len(results) != 1 {
		t.Errorf("Incorrect number of events: %d", len(results))
	}

	if !reflect.DeepEqual(results[0], content) {
		t.Errorf("Expected origional content \"%v\", but received \"%v\"", content, results[0])
	}
}

type Test struct {
	Value int
}

func TestBadgerEventStore_AppendWithMultipleFacts(t *testing.T) {
	store := MemoryStore()
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()

	store.Register(Test{})

	fact1 := "test.1"
	fact2 := "test.2"

	for i := 0; i < 3; i++ {
		value := Test{Value: i}

		err := store.Append(fact1, value)
		if err != nil {
			t.Errorf("Failed append %s:", err)
			break
		}
	}

	for i := 0; i < 9; i++ {
		value := Test{Value: i}

		err := store.Append(fact2, value)
		if err != nil {
			t.Errorf("Failed append %s:", err)
			break
		}
	}

	results1, err := store.Read(fact1)
	if err != nil {
		t.Error(err)
	}

	if len(results1) != 3 {
		t.Error(store.ListKeysForAggregate(fact1))
		t.Errorf("Incorrect number of events: %d", len(results1))
	}

	results2, err := store.Read(fact2)
	if err != nil {
		t.Error(err)
	}

	if len(results2) != 9 {
		t.Error(store.ListKeysForAggregate(fact2))
		t.Errorf("Incorrect number of events: %d", len(results2))
	}

	keys, err := store.ListKeys()
	if err != nil {
		t.Error(err)
	}

	for _, k := range keys {
		fmt.Println(k)
	}
}
