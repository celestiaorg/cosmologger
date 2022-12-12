package tools

import (
	"fmt"

	"github.com/davecgh/go-spew/spew"
)

func UniqueStringSlice(input []string) []string {

	strMap := map[string]bool{}
	for _, s := range input {
		strMap[s] = true
	}

	output := []string{}
	for s := range strMap {
		output = append(output, s)
	}

	return output
}

// useful for debugging
func PrettyPrint(v any, prefix ...string) {

	if len(prefix) > 0 {
		fmt.Printf("%s: ", prefix)
	}

	spew.Config.MaxDepth = 1
	spew.Config.Indent = "\t"
	spew.Dump(v)
}
