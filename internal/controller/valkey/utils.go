package valkey

import (
	"fmt"
	"strconv"
	"strings"
)

type ClusterNode struct {
	Pod          string
	IP           string
	ID           string
	MasterNodeID string
	Flags        []string
	SlotRanges   []*ClusterSlotRange
}

func parseClusterNodeLine(line string) (*ClusterNode, error) {
	strings.Fields(line)
	fields := strings.Fields(line)
	if len(fields) < 4 {
		return nil, fmt.Errorf("expected len(fields) >= 4, but got %d: %v", len(fields), fields)
	}

	flagsWithoutMyself := []string{}
	flags := strings.Split(fields[2], ",")
	for _, flag := range flags {
		if flag != "myself" {
			flagsWithoutMyself = append(flagsWithoutMyself, flag)
		}
	}
	slotRanges := make([]*ClusterSlotRange, 0)
	if len(fields) > 8 {
		for i := 8; i < len(fields); i++ {
			if strings.Contains(fields[i], "-") {
				parts := strings.Split(fields[i], "-")
				start, err := strconv.Atoi(parts[0])
				if err != nil {
					return nil, err
				}
				end, err := strconv.Atoi(parts[1])
				if err != nil {
					return nil, err
				}
				slotRange := &ClusterSlotRange{
					Start: start,
					End:   end,
				}
				slotRanges = append(slotRanges, slotRange)
			} else {
				start, err := strconv.Atoi(fields[i])
				if err != nil {
					return nil, err
				}
				end := start
				slotRange := &ClusterSlotRange{
					Start: start,
					End:   end,
				}
				slotRanges = append(slotRanges, slotRange)
			}
		}
	}
	IP := strings.Split(fields[1], ":")[0]
	ID := strings.ReplaceAll(fields[0], "txt:", "")
	MasterNodeID := fields[3]
	if MasterNodeID == "-" {
		MasterNodeID = ""
	}
	return &ClusterNode{
		IP:           IP,
		ID:           ID,
		MasterNodeID: MasterNodeID,
		Flags:        flagsWithoutMyself,
		SlotRanges:   slotRanges,
	}, nil
}

func ParseClusterNodes(clusterNodesTxt string) ([]*ClusterNode, error) {
	result := make([]*ClusterNode, 0)
	for _, line := range strings.Split(clusterNodesTxt, "\n") {
		clusterNode, err := parseClusterNodeLine(line)
		if err != nil {
			return nil, err
		}
		result = append(result, clusterNode)
	}
	return result, nil
}

func ParseClusterNode(clusterNodesTxt string) (*ClusterNode, error) {
	for _, line := range strings.Split(clusterNodesTxt, "\n") {
		if strings.Contains(line, "myself") {
			return parseClusterNodeLine(line)
		}
	}
	return nil, fmt.Errorf("Could not parse cluster nodes from text: %s", clusterNodesTxt)
}

func ParseClusterNodesExludeSelf(clusterNodesTxt string) ([]*ClusterNode, error) {
	result := make([]*ClusterNode, 0)
	for _, line := range strings.Split(clusterNodesTxt, "\n") {
		if strings.Contains(line, "myself") {
			continue
		}
		if line == "" {
			continue
		}
		clusterNode, err := parseClusterNodeLine(line)
		if err != nil {
			return nil, err
		}
		result = append(result, clusterNode)
	}
	return result, nil
}

// There are 16384 hash slots in Valkey Cluster, and to compute the hash slot for a given key, we simply take the CRC16 of the key modulo 16384.
// 0-16383

type ClusterSlotRange struct {
	Start int
	End   int
}

func (c *ClusterSlotRange) String() string {
	if c == nil {
		return "-"
	}
	return fmt.Sprintf("%d-%d", c.Start, c.End)
}

func SlotRanges(numShards int) []ClusterSlotRange {
	hashSlots := 16384
	if numShards < 1 {
		return nil
	}

	perGroup := hashSlots / numShards

	result := make([]ClusterSlotRange, 0)
	j := 0
	for i := 0; i < numShards; i++ {
		if i == numShards-1 {
			result = append(result, ClusterSlotRange{Start: j, End: 16383})
			return result
		}
		result = append(result, ClusterSlotRange{Start: j, End: j + perGroup - 1})
		j = j + perGroup
	}
	return result
}

func SlotCounts(numShards int) []int {
	ranges := SlotRanges(numShards)
	counts := make([]int, 0)
	for _, r := range ranges {
		counts = append(counts, (r.End-r.Start)+1)
	}
	return counts
}
