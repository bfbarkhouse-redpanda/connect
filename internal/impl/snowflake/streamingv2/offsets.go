// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package streamingv2

import "strconv"

// CompareOffsetTokens orders two offset tokens, returning -1, 0 or +1.
//
// Offset tokens are opaque strings to Snowflake; ordering is the client's
// responsibility. When both sides parse as int64 we compare numerically, which
// is what Kafka offsets need — a string comparison puts "10" before "9" and
// makes exactly-once filtering wrong from offset 10 onward. Anything that does
// not parse falls back to lexicographic comparison, preserving upstream
// behaviour for non-numeric tokens.
func CompareOffsetTokens(a, b string) int {
	ai, aerr := strconv.ParseInt(a, 10, 64)
	bi, berr := strconv.ParseInt(b, 10, 64)
	if aerr == nil && berr == nil {
		switch {
		case ai < bi:
			return -1
		case ai > bi:
			return 1
		default:
			return 0
		}
	}
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
