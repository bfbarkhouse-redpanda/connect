// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package streamingv2

import "testing"

func TestCompareOffsetTokensNumeric(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"9", "10", -1}, // the bug: lexicographic says +1
		{"10", "9", 1},
		{"7", "7", 0},
		{"0", "9223372036854775807", -1}, // int64 max
		{"9223372036854775807", "9223372036854775806", 1},
		{"00010", "10", 0}, // leading zeros are numerically equal
	}
	for _, c := range cases {
		if got := CompareOffsetTokens(c.a, c.b); got != c.want {
			t.Errorf("CompareOffsetTokens(%q,%q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestCompareOffsetTokensFallsBackToStringWhenNotNumeric(t *testing.T) {
	// Preserves upstream behaviour for non-numeric tokens.
	if got := CompareOffsetTokens("abc", "abd"); got != -1 {
		t.Errorf("non-numeric compare = %d, want -1", got)
	}
	if got := CompareOffsetTokens("10", "abc"); got != -1 {
		t.Errorf("mixed compare should fall back to string: got %d, want -1", got)
	}
	if got := CompareOffsetTokens("", ""); got != 0 {
		t.Errorf("empty compare = %d, want 0", got)
	}
}

func TestCompareOffsetTokensRejectsOverflowAsNonNumeric(t *testing.T) {
	// Beyond int64: must not wrap or panic. Falls back to string comparison.
	huge := "99999999999999999999"
	if got := CompareOffsetTokens(huge, "1"); got != 1 {
		t.Errorf("overflow compare = %d, want 1 (string fallback)", got)
	}
}
