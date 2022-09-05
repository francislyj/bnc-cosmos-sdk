package slashing

import (
	"encoding/binary"
	"fmt"
	sdk "github.com/cosmos/cosmos-sdk/types"
	abci "github.com/tendermint/tendermint/abci/types"
	tmtypes "github.com/tendermint/tendermint/types"
)

// slashing begin block functionality
func BeginBlocker(ctx sdk.Context, req abci.RequestBeginBlock, sk Keeper) (tags sdk.Tags) {

	// Tag the height
	heightBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(heightBytes, uint64(req.Header.Height))
	tags = sdk.NewTags("height", heightBytes)

	// Iterate over all the validators  which *should* have signed this block
	// store whether or not they have actually signed it and slash/unbond any
	// which have missed too many blocks in a row (downtime slashing)
	fmt.Println("slashing begin block liveness --------------------------------> begin")
	for _, voteInfo := range req.LastCommitInfo.GetVotes() {
		//fmt.Printf("liveness address %v, isSignedLastBlock %v \n", sdk.HexEncode(voteInfo.Validator.GetAddress()), voteInfo.SignedLastBlock)
		sk.handleBeaconChainValidatorSignature(ctx, voteInfo.Validator.Address, voteInfo.Validator.Power, voteInfo.SignedLastBlock)
	}
	fmt.Println("slashing begin block liveness --------------------------------> end")
	// Iterate through any newly discovered evidence of infraction
	// Slash any validators (and since-unbonded stake within the unbonding period)
	// who contributed to valid infractions
	fmt.Println("slashing begin block doublesign --------------------------------> begin")
	for _, evidence := range req.ByzantineValidators {
		switch evidence.Type {
		case tmtypes.ABCIEvidenceTypeDuplicateVote:
			//sk.handleDoubleSign(ctx, evidence.Validator.Address, evidence.Height, evidence.Time, evidence.Validator.Power)
			sk.handleBeaconChainDoubleSign(ctx, evidence.Validator.Address, evidence.Height, evidence.Time, evidence.Validator.Power)
		default:
			ctx.Logger().With("module", "x/slashing").Error(fmt.Sprintf("ignored unknown evidence type: %s", evidence.Type))
		}
	}
	fmt.Println("slashing begin block doublesign --------------------------------> end")
	return
}
