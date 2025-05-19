package valkey

import (
	"fmt"
	"testing"
)

func TestSlotRanges(t *testing.T) {
	testcases := []struct {
		in  int
		out []ClusterSlotRange
	}{
		{1, []ClusterSlotRange{{0, 16383}}},
		{2, []ClusterSlotRange{{0, 8191}, {8192, 16383}}},
		{3, []ClusterSlotRange{{0, 5460}, {5461, 10921}, {10922, 16383}}},
		{4, []ClusterSlotRange{{0, 4095}, {4096, 8191}, {8192, 12287}, {12288, 16383}}},
	}

	for _, tt := range testcases {
		t.Run(fmt.Sprintf("%d", tt.in), func(t *testing.T) {
			ranges := SlotRanges(tt.in)

			if len(ranges) != len(tt.out) {
				t.Errorf("expected len(ranges) == len(tt.out), but got %d != %d", len(ranges), len(tt.out))
			}
			for i, actual := range ranges {
				if actual.Start != tt.out[i].Start {
					t.Errorf("got %d, want %d", actual.Start, tt.out[i].Start)
				}
				if actual.End != tt.out[i].End {
					t.Errorf("got %d, want %d", actual.End, tt.out[i].End)
				}
			}
		})
	}

}
