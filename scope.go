package frisbii

import (
	"fmt"

	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

// TODO: this is temporary, it needs to belong in a shared package, likely go-car/v2/trustless or similar

type CarScope string

const CarScopeAll CarScope = "all"
const CarScopeFile CarScope = "file"
const CarScopeBlock CarScope = "block"

var matcherSelector = builder.NewSelectorSpecBuilder(basicnode.Prototype.Any).Matcher()

func (cs CarScope) TerminalSelectorSpec() builder.SelectorSpec {
	switch cs {
	case CarScopeAll:
		return unixfsnode.ExploreAllRecursivelySelector
	case CarScopeFile:
		return unixfsnode.MatchUnixFSPreloadSelector // file
	case CarScopeBlock:
		return matcherSelector
	case CarScope(""):
		return unixfsnode.ExploreAllRecursivelySelector // default to explore-all for zero-value CarScope
	}
	panic(fmt.Sprintf("unknown CarScope: [%s]", string(cs)))
}

func (cs CarScope) AcceptHeader() string {
	switch cs {
	case CarScopeBlock:
		return "application/vnd.ipld.block"
	default:
		return "application/vnd.ipld.car"
	}
}
