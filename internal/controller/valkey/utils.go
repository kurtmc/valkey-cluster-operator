package valkey

import (
	"fmt"
	"strings"
)

type ClusterNode struct {
	Pod          string
	IP           string
	ID           string
	MasterNodeID string
	Flags        []string
	SlotRange    string
}

func ParseClusterNode(clusterNodesTxt string) (*ClusterNode, error) {
	for _, line := range strings.Split(clusterNodesTxt, "\n") {
		if strings.Contains(line, "myself") {
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
			slotRange := ""
			if len(fields) > 8 {
				slotRange = fields[8]
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
				SlotRange:    slotRange,
			}, nil
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
		slotRange := ""
		if len(fields) > 8 {
			slotRange = fields[8]
		}
		IP := strings.Split(fields[1], ":")[0]
		ID := strings.ReplaceAll(fields[0], "txt:", "")
		MasterNodeID := fields[3]
		if MasterNodeID == "-" {
			MasterNodeID = ""
		}
		result = append(result, &ClusterNode{
			IP:           IP,
			ID:           ID,
			MasterNodeID: MasterNodeID,
			Flags:        flagsWithoutMyself,
			SlotRange:    slotRange,
		})
	}
	return result, nil
}
