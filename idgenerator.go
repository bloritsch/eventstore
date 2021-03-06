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
	"github.com/oklog/ulid/v2"
	"math/rand"
	"time"
)

func NewId(t time.Time) ulid.ULID {
	source := rand.NewSource(t.UnixNano())
	rnd := rand.New(source)
	entropy := ulid.Monotonic(rnd, 0)
	return ulid.MustNew(ulid.Timestamp(t), entropy)
}
