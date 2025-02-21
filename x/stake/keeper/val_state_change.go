package keeper

import (
	"bytes"
	"fmt"
	"sort"

	abci "github.com/tendermint/tendermint/abci/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/stake/types"
)

// Apply and return accumulated updates to the bonded validator set. Also,
// * Updates the active valset as keyed by LastValidatorPowerKey.
// * Updates the total power as keyed by LastTotalPowerKey.
// * Updates validator status' according to updated powers.
// * Updates the fee pool bonded vs loose tokens.
// * Updates relevant indices.
// It gets called once after genesis, another time maybe after genesis transactions,
// then once at every EndBlock.
//
// CONTRACT: Only validators with non-zero power or zero-power that were bonded
// at the previous block height or were removed from the validator set entirely
// are returned to Tendermint.
// CONTRACT: When handle the side chain validators, `updates` is not collected
func (k Keeper) ApplyAndReturnValidatorSetUpdates(ctx sdk.Context) (newVals []types.Validator, updates []abci.ValidatorUpdate) {
	store := ctx.KVStore(k.storeKey)
	maxValidators := k.GetParams(ctx).MaxValidators
	var totalPower int64

	// Retrieve the last validator set.
	// The persistent set is updated later in this function.
	// (see LastValidatorPowerKey).
	last := k.getLastValidatorsByAddr(ctx)

	newVals = make([]types.Validator, 0, maxValidators)
	// Iterate over validators, highest power to lowest.
	iterator := sdk.KVStoreReversePrefixIterator(store, ValidatorsByPowerIndexKey)
	defer iterator.Close()
	count := 0
	for ; iterator.Valid() && count < int(maxValidators); iterator.Next() {

		// fetch the validator
		operator := sdk.ValAddress(iterator.Value())
		validator := k.mustGetValidator(ctx, operator)

		if validator.Jailed {
			panic("should never retrieve a jailed validator from the power store")
		}

		// if we get to a zero-power validator (which we don't bond),
		// there are no more possible bonded validators
		// note: we must check the ABCI power, since we round before sending to Tendermint
		if validator.Tokens.RawInt() == int64(0) {
			break
		}

		// apply the appropriate state change if necessary
		switch validator.Status {
		case sdk.Unbonded:
			validator = k.unbondedToBonded(ctx, validator)
		case sdk.Unbonding:
			validator = k.unbondingToBonded(ctx, validator)
		case sdk.Bonded:
			// no state change
		default:
			panic("unexpected validator status")
		}

		newVals = append(newVals, validator)

		// fetch the old power bytes
		var operatorBytes [sdk.AddrLen]byte
		copy(operatorBytes[:], operator[:])
		oldPowerBytes, found := last[operatorBytes]

		// calculate the new power bytes
		newPower := validator.BondedTokens().RawInt()
		newPowerBytes := k.cdc.MustMarshalBinaryLengthPrefixed(newPower)
		// update the validator set if power has changed
		if !found || !bytes.Equal(oldPowerBytes, newPowerBytes) {
			// Note: side chain validators do not have ConsPubKey, and we do not need to collect the updates as well.
			if validator.ConsPubKey != nil {
				updates = append(updates, validator.ABCIValidatorUpdate())
			}
			// set validator power on lookup index.
			k.SetLastValidatorPower(ctx, operator, newPower)
		}

		// validator still in the validator set, so delete from the copy
		delete(last, operatorBytes)

		// keep count
		count++
		totalPower = totalPower + newPower
	}

	// sort the no-longer-bonded validators
	noLongerBonded := k.sortNoLongerBonded(last)

	// iterate through the sorted no-longer-bonded validators
	for _, operator := range noLongerBonded {

		// fetch the validator
		validator := k.mustGetValidator(ctx, sdk.ValAddress(operator))

		// bonded to unbonding
		k.bondedToUnbonding(ctx, validator)

		// remove validator if it has no more tokens
		if validator.Tokens.IsZero() {
			k.RemoveValidator(ctx, validator.OperatorAddr)
		}

		// delete from the bonded validator index
		k.DeleteLastValidatorPower(ctx, sdk.ValAddress(operator))

		// update the validator set
		if validator.ConsPubKey != nil {
			updates = append(updates, validator.ABCIValidatorUpdateZero())
		}
	}

	// set total power on lookup index if there are any updates
	if len(updates) > 0 {
		k.SetLastTotalPower(ctx, totalPower)
	}

	return newVals, updates
}

// Validator state transitions

func (k Keeper) bondedToUnbonding(ctx sdk.Context, validator types.Validator) types.Validator {
	if validator.Status != sdk.Bonded {
		panic(fmt.Sprintf("bad state transition bondedToUnbonding, validator: %v\n", validator))
	}
	return k.beginUnbondingValidator(ctx, validator)
}

func (k Keeper) unbondingToBonded(ctx sdk.Context, validator types.Validator) types.Validator {
	if validator.Status != sdk.Unbonding {
		panic(fmt.Sprintf("bad state transition unbondingToBonded, validator: %v\n", validator))
	}
	return k.bondValidator(ctx, validator)
}

func (k Keeper) unbondedToBonded(ctx sdk.Context, validator types.Validator) types.Validator {
	if validator.Status != sdk.Unbonded {
		panic(fmt.Sprintf("bad state transition unbondedToBonded, validator: %v\n", validator))
	}
	return k.bondValidator(ctx, validator)
}

// switches a validator from unbonding state to unbonded state
func (k Keeper) unbondingToUnbonded(ctx sdk.Context, validator types.Validator) types.Validator {
	if validator.Status != sdk.Unbonding {
		panic(fmt.Sprintf("bad state transition unbondingToBonded, validator: %v\n", validator))
	}
	return k.completeUnbondingValidator(ctx, validator)
}

// send a validator to jail
func (k Keeper) jailValidator(ctx sdk.Context, validator types.Validator) {
	if validator.Jailed {
		panic(fmt.Sprintf("cannot jail already jailed validator, validator: %v\n", validator))
	}

	validator.Jailed = true
	k.SetValidator(ctx, validator)
	k.DeleteValidatorByPowerIndex(ctx, validator)
}

// remove a validator from jail
func (k Keeper) unjailValidator(ctx sdk.Context, validator types.Validator) {
	if !validator.Jailed {
		panic(fmt.Sprintf("cannot unjail already unjailed validator, validator: %v\n", validator))
	}

	validator.Jailed = false
	k.SetValidator(ctx, validator)
	k.SetValidatorByPowerIndex(ctx, validator)
}

// perform all the store operations for when a validator status becomes bonded
func (k Keeper) bondValidator(ctx sdk.Context, validator types.Validator) types.Validator {

	pool := k.GetPool(ctx)

	k.DeleteValidatorByPowerIndex(ctx, validator)

	validator.BondHeight = ctx.BlockHeight()

	// set the status
	validator, pool = validator.UpdateStatus(pool, sdk.Bonded)
	k.SetPool(ctx, pool)

	// save the now bonded validator record to the three referenced stores
	k.SetValidator(ctx, validator)

	k.SetValidatorByPowerIndex(ctx, validator)

	// call the bond hook if present
	if k.hooks != nil {
		if validator.IsSideChainValidator() {
			k.hooks.OnSideChainValidatorBonded(ctx, validator.SideConsAddr, validator.OperatorAddr)
		} else {
			k.hooks.OnValidatorBonded(ctx, validator.ConsAddress(), validator.OperatorAddr)
		}
	}

	return validator
}

// perform all the store operations for when a validator status begins unbonding
func (k Keeper) beginUnbondingValidator(ctx sdk.Context, validator types.Validator) types.Validator {

	pool := k.GetPool(ctx)
	params := k.GetParams(ctx)

	k.DeleteValidatorByPowerIndex(ctx, validator)

	// sanity check
	if validator.Status != sdk.Bonded {
		panic(fmt.Sprintf("should not already be unbonded or unbonding, validator: %v\n", validator))
	}

	// set the status
	validator, pool = validator.UpdateStatus(pool, sdk.Unbonding)
	k.SetPool(ctx, pool)

	validator.UnbondingMinTime = ctx.BlockHeader().Time.Add(params.UnbondingTime)
	validator.UnbondingHeight = ctx.BlockHeader().Height

	// save the now unbonded validator record
	k.SetValidator(ctx, validator)

	k.SetValidatorByPowerIndex(ctx, validator)

	// Adds to unbonding validator queue
	k.InsertValidatorQueue(ctx, validator)

	// call the unbond hook if present
	if k.hooks != nil {
		if validator.IsSideChainValidator() {
			k.hooks.OnSideChainValidatorBeginUnbonding(ctx, validator.SideConsAddr, validator.OperatorAddr)
		} else {
			k.hooks.OnValidatorBeginUnbonding(ctx, validator.ConsAddress(), validator.OperatorAddr)
		}
	}

	return validator
}

// perform all the store operations for when a validator status becomes unbonded
func (k Keeper) completeUnbondingValidator(ctx sdk.Context, validator types.Validator) types.Validator {
	pool := k.GetPool(ctx)
	validator, pool = validator.UpdateStatus(pool, sdk.Unbonded)
	k.SetPool(ctx, pool)
	k.SetValidator(ctx, validator)
	return validator
}

// map of operator addresses to serialized power
type validatorsByAddr map[[sdk.AddrLen]byte][]byte

// get the last validator set
func (k Keeper) getLastValidatorsByAddr(ctx sdk.Context) validatorsByAddr {
	last := make(validatorsByAddr)
	store := ctx.KVStore(k.storeKey)
	iterator := sdk.KVStorePrefixIterator(store, LastValidatorPowerKey)
	defer iterator.Close()
	for ; iterator.Valid(); iterator.Next() {
		var operator [sdk.AddrLen]byte
		copy(operator[:], iterator.Key()[1:])
		powerBytes := iterator.Value()
		last[operator] = make([]byte, len(powerBytes))
		copy(last[operator][:], powerBytes[:])
	}
	return last
}

// given a map of remaining validators to previous bonded power
// returns the list of validators to be unbonded, sorted by operator address
func (k Keeper) sortNoLongerBonded(last validatorsByAddr) [][]byte {
	// sort the map keys for determinism
	noLongerBonded := make([][]byte, len(last))
	index := 0
	for operatorBytes := range last {
		operator := make([]byte, sdk.AddrLen)
		copy(operator[:], operatorBytes[:])
		noLongerBonded[index] = operator
		index++
	}
	// sorted by address - order doesn't matter
	sort.SliceStable(noLongerBonded, func(i, j int) bool {
		return bytes.Compare(noLongerBonded[i], noLongerBonded[j]) == -1
	})
	return noLongerBonded
}
