package keeper

import (
	"encoding/binary"
	"fmt"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/stake/types"
)

// get a single validator
func (k Keeper) GetValidator(ctx sdk.Context, addr sdk.ValAddress) (validator types.Validator, found bool) {
	store := ctx.KVStore(k.storeKey)
	value := store.Get(GetValidatorKey(addr))
	if value == nil {
		return validator, false
	}

	validator = types.MustUnmarshalValidator(k.cdc, value)
	return validator, true
}

func (k Keeper) mustGetValidator(ctx sdk.Context, addr sdk.ValAddress) types.Validator {
	validator, found := k.GetValidator(ctx, addr)
	if !found {
		panic(fmt.Sprintf("validator record not found for address: %X\n", addr))
	}
	return validator
}

// get a single validator by consensus address
func (k Keeper) GetValidatorByConsAddr(ctx sdk.Context, consAddr sdk.ConsAddress) (validator types.Validator, found bool) {
	store := ctx.KVStore(k.storeKey)
	opAddr := store.Get(GetValidatorByConsAddrKey(consAddr))
	if opAddr == nil {
		return validator, false
	}
	return k.GetValidator(ctx, opAddr)
}

func (k Keeper) mustGetValidatorByConsAddr(ctx sdk.Context, consAddr sdk.ConsAddress) types.Validator {
	validator, found := k.GetValidatorByConsAddr(ctx, consAddr)
	if !found {
		panic(fmt.Errorf("validator with consensus-Address %s not found", consAddr))
	}
	return validator
}

func (k Keeper) GetValidatorBySideConsAddr(ctx sdk.Context, sideConsAddr []byte) (validator types.Validator, found bool) {
	store := ctx.KVStore(k.storeKey)
	opAddr := store.Get(GetValidatorBySideConsAddrKey(sideConsAddr))
	if opAddr == nil {
		return validator, false
	}
	return k.GetValidator(ctx, opAddr)
}

func (k Keeper) mustGetValidatorBySideConsAddr(ctx sdk.Context, sideConsAddr []byte) types.Validator {
	store := ctx.KVStore(k.storeKey)
	opAddr := store.Get(GetValidatorBySideConsAddrKey(sideConsAddr))
	if opAddr == nil {
		panic(fmt.Errorf("validator with consensus-Address %s not found", sideConsAddr))
	}
	return k.mustGetValidator(ctx, opAddr)
}

func (k Keeper) GetValidatorsByHeight(ctx sdk.Context, height int64) (validators []types.Validator, found bool) {
	store := ctx.KVStore(k.storeKey)
	bz := store.Get(GetValidatorHeightKey(height))
	if bz == nil {
		return validators, false
	}
	validators = types.MustUnmarshalValidators(k.cdc, bz)
	return validators, true
}

func (k Keeper) GetHeightValidatorsByIndex(ctx sdk.Context, indexCountBackwards int) ([]types.Validator, int64, bool) {
	store := ctx.KVStore(k.storeKey)
	iterator := sdk.KVStoreReversePrefixIterator(store, ValidatorsByHeightKey)
	defer iterator.Close()

	index := 1
	for ; iterator.Valid(); iterator.Next() {
		if index == indexCountBackwards {
			key := iterator.Key()
			validators := types.MustUnmarshalValidators(k.cdc, iterator.Value())
			return validators, int64(binary.BigEndian.Uint64(key[len(ValidatorsByHeightKey):])), true
		}
		index++
	}
	return nil, 0, false
}

func (k Keeper) GetEarliestValidatorsWithHeight(ctx sdk.Context) ([]types.Validator, bool) {
	store := ctx.KVStore(k.storeKey)
	iterator := sdk.KVStorePrefixIterator(store, ValidatorsByHeightKey)
	defer iterator.Close()

	for iterator.Valid() {
		validators := types.MustUnmarshalValidators(k.cdc, iterator.Value())
		return validators, true
	}
	return nil, false
}

func (k Keeper) ExistValidatorsWithHeight(ctx sdk.Context, height int64) bool {
	store := ctx.KVStore(k.storeKey)
	return store.Has(GetValidatorHeightKey(height))
}

func (k Keeper) ExistHeightValidators(ctx sdk.Context) bool {
	store := ctx.KVStore(k.storeKey)
	iterator := sdk.KVStorePrefixIterator(store, ValidatorsByHeightKey)
	defer iterator.Close()
	return iterator.Valid()
}

//___________________________________________________________________________

// set the main record holding validator details
func (k Keeper) SetValidator(ctx sdk.Context, validator types.Validator) {
	store := ctx.KVStore(k.storeKey)
	bz := types.MustMarshalValidator(k.cdc, validator)
	store.Set(GetValidatorKey(validator.OperatorAddr), bz)
	// publish validator update
	if k.PbsbServer != nil && ctx.IsDeliverTx() {
		k.PbsbServer.Publish(types.ValidatorUpdateEvent{
			StakeEvent: types.StakeEvent{
				IsFromTx: ctx.Tx() != nil,
			},
			Validator: validator,
		})
	}
}

// validator index
func (k Keeper) SetValidatorByConsAddr(ctx sdk.Context, validator types.Validator) {
	store := ctx.KVStore(k.storeKey)
	if validator.IsSideChainValidator() {
		store.Set(GetValidatorByConsAddrKey(validator.SideConsAddr), validator.OperatorAddr)
	} else {
		consAddr := sdk.ConsAddress(validator.ConsPubKey.Address())
		store.Set(GetValidatorByConsAddrKey(consAddr), validator.OperatorAddr)
	}
}

// validator index
func (k Keeper) SetValidatorByPowerIndex(ctx sdk.Context, validator types.Validator) {
	// jailed validators are not kept in the power index
	if validator.Jailed {
		return
	}
	store := ctx.KVStore(k.storeKey)
	store.Set(GetValidatorsByPowerIndexKey(validator), validator.OperatorAddr)
}

// validator index
func (k Keeper) DeleteValidatorByPowerIndex(ctx sdk.Context, validator types.Validator) {
	store := ctx.KVStore(k.storeKey)
	store.Delete(GetValidatorsByPowerIndexKey(validator))
}

// validator index
func (k Keeper) SetNewValidatorByPowerIndex(ctx sdk.Context, validator types.Validator) {
	store := ctx.KVStore(k.storeKey)
	store.Set(GetValidatorsByPowerIndexKey(validator), validator.OperatorAddr)
}

func (k Keeper) SetValidatorsByHeight(ctx sdk.Context, height int64, validators []types.Validator) {
	store := ctx.KVStore(k.storeKey)
	bz := types.MustMarshalValidators(k.cdc, validators)
	store.Set(GetValidatorHeightKey(height), bz)
}

//___________________________________________________________________________

// Update the tokens of an existing validator, update the validators power index key
func (k Keeper) AddValidatorTokensAndShares(ctx sdk.Context, validator types.Validator,
	tokensToAdd int64) (valOut types.Validator, addedShares sdk.Dec) {

	pool := k.GetPool(ctx)
	k.DeleteValidatorByPowerIndex(ctx, validator)
	validator, pool, addedShares = validator.AddTokensFromDel(pool, tokensToAdd)
	// increment the intra-tx counter
	// in case of a conflict, the validator which least recently changed power takes precedence
	counter := k.GetIntraTxCounter(ctx)
	validator.BondIntraTxCounter = counter
	k.SetIntraTxCounter(ctx, counter+1)
	k.SetValidator(ctx, validator)
	k.SetPool(ctx, pool)
	k.SetValidatorByPowerIndex(ctx, validator)
	return validator, addedShares
}

// Update the tokens of an existing validator, update the validators power index key
func (k Keeper) RemoveValidatorTokensAndShares(ctx sdk.Context, validator types.Validator,
	sharesToRemove sdk.Dec) (valOut types.Validator, removedTokens sdk.Dec) {

	pool := k.GetPool(ctx)
	k.DeleteValidatorByPowerIndex(ctx, validator)
	validator, pool, removedTokens = validator.RemoveDelShares(pool, sharesToRemove)
	k.SetValidator(ctx, validator)
	k.SetPool(ctx, pool)
	k.SetValidatorByPowerIndex(ctx, validator)
	return validator, removedTokens
}

// Update the tokens of an existing validator, update the validators power index key
func (k Keeper) RemoveValidatorTokens(ctx sdk.Context, validator types.Validator, tokensToRemove sdk.Dec) types.Validator {
	pool := k.GetPool(ctx)
	k.DeleteValidatorByPowerIndex(ctx, validator)
	validator, pool = validator.RemoveTokens(pool, tokensToRemove)
	k.SetValidator(ctx, validator)
	k.SetPool(ctx, pool)
	k.SetValidatorByPowerIndex(ctx, validator)
	return validator
}

// UpdateValidatorCommission attempts to update a validator's commission rate.
// An error is returned if the new commission rate is invalid.
func (k Keeper) UpdateValidatorCommission(ctx sdk.Context, validator types.Validator, newRate sdk.Dec) (types.Commission, sdk.Error) {
	commission := validator.Commission
	blockTime := ctx.BlockHeader().Time

	if err := commission.ValidateNewRate(newRate, blockTime); err != nil {
		return commission, err
	}

	commission.Rate = newRate
	commission.UpdateTime = blockTime

	return commission, nil
}

// remove the validator record and associated indexes
// except for the bonded validator index which is only handled in ApplyAndReturnTendermintUpdates
func (k Keeper) RemoveValidator(ctx sdk.Context, address sdk.ValAddress) {

	// first retrieve the old validator record
	validator, found := k.GetValidator(ctx, address)
	if !found {
		return
	}

	// delete the old validator record
	store := ctx.KVStore(k.storeKey)
	store.Delete(GetValidatorKey(address))
	if validator.IsSideChainValidator() {
		store.Delete(GetValidatorByConsAddrKey(validator.SideConsAddr))
	} else {
		store.Delete(GetValidatorByConsAddrKey(sdk.ConsAddress(validator.ConsPubKey.Address())))
	}
	store.Delete(GetValidatorsByPowerIndexKey(validator))

	// publish validator update
	if k.PbsbServer != nil && ctx.IsDeliverTx() {
		k.PbsbServer.Publish(types.ValidatorRemovedEvent{
			StakeEvent: types.StakeEvent{
				IsFromTx: ctx.Tx() != nil,
			},
			Operator:    validator.OperatorAddr,
			SideChainId: validator.SideChainId,
		})
	}
}

// remove the validators stored with key of height
func (k Keeper) RemoveValidatorsByHeight(ctx sdk.Context, height int64) {

	if !k.ExistValidatorsWithHeight(ctx, height) {
		return
	}

	store := ctx.KVStore(k.storeKey)
	store.Delete(GetValidatorHeightKey(height))
}

//___________________________________________________________________________
// get groups of validators

// get the set of all validators with no limits, used during genesis dump
func (k Keeper) GetAllValidators(ctx sdk.Context) (validators []types.Validator) {
	store := ctx.KVStore(k.storeKey)
	iterator := sdk.KVStorePrefixIterator(store, ValidatorsKey)
	defer iterator.Close()

	for ; iterator.Valid(); iterator.Next() {
		validator := types.MustUnmarshalValidator(k.cdc, iterator.Value())
		validators = append(validators, validator)
	}
	return validators
}

// return a given amount of all the validators
func (k Keeper) GetValidators(ctx sdk.Context, maxRetrieve uint16) (validators []types.Validator) {
	store := ctx.KVStore(k.storeKey)
	validators = make([]types.Validator, maxRetrieve)

	iterator := sdk.KVStorePrefixIterator(store, ValidatorsKey)
	defer iterator.Close()

	i := 0
	for ; iterator.Valid() && i < int(maxRetrieve); iterator.Next() {
		validator := types.MustUnmarshalValidator(k.cdc, iterator.Value())
		validators[i] = validator
		i++
	}
	return validators[:i] // trim if the array length < maxRetrieve
}

// get the group of the bonded validators
func (k Keeper) GetLastValidators(ctx sdk.Context) (validators []types.Validator) {
	store := ctx.KVStore(k.storeKey)

	// add the actual validator power sorted store
	maxValidators := k.MaxValidators(ctx)
	validators = make([]types.Validator, maxValidators)

	iterator := sdk.KVStorePrefixIterator(store, LastValidatorPowerKey)
	defer iterator.Close()

	i := 0
	for ; iterator.Valid(); iterator.Next() {

		// sanity check
		if i >= int(maxValidators) {
			panic("more validators than maxValidators found")
		}
		address := AddressFromLastValidatorPowerKey(iterator.Key())
		validator := k.mustGetValidator(ctx, address)

		validators[i] = validator
		i++
	}
	return validators[:i] // trim
}

// get the current group of bonded validators sorted by power-rank
func (k Keeper) GetBondedValidatorsByPower(ctx sdk.Context) []types.Validator {
	store := ctx.KVStore(k.storeKey)
	maxValidators := k.MaxValidators(ctx)
	validators := make([]types.Validator, maxValidators)

	iterator := sdk.KVStoreReversePrefixIterator(store, ValidatorsByPowerIndexKey)
	defer iterator.Close()

	i := 0
	for ; iterator.Valid() && i < int(maxValidators); iterator.Next() {
		address := iterator.Value()
		validator := k.mustGetValidator(ctx, address)

		if validator.Status == sdk.Bonded {
			validators[i] = validator
			i++
		}
	}
	return validators[:i] // trim
}

// gets a specific validator queue timeslice. A timeslice is a slice of ValAddresses corresponding to unbonding validators
// that expire at a certain time.
func (k Keeper) GetValidatorQueueTimeSlice(ctx sdk.Context, timestamp time.Time) (valAddrs []sdk.ValAddress) {
	store := ctx.KVStore(k.storeKey)
	bz := store.Get(GetValidatorQueueTimeKey(timestamp))
	if bz == nil {
		return []sdk.ValAddress{}
	}
	k.cdc.MustUnmarshalBinaryLengthPrefixed(bz, &valAddrs)
	return valAddrs
}

// Sets a specific validator queue timeslice.
func (k Keeper) SetValidatorQueueTimeSlice(ctx sdk.Context, timestamp time.Time, keys []sdk.ValAddress) {
	store := ctx.KVStore(k.storeKey)
	bz := k.cdc.MustMarshalBinaryLengthPrefixed(keys)
	store.Set(GetValidatorQueueTimeKey(timestamp), bz)
}

// Insert an validator address to the appropriate timeslice in the validator queue
func (k Keeper) InsertValidatorQueue(ctx sdk.Context, val types.Validator) {
	timeSlice := k.GetValidatorQueueTimeSlice(ctx, val.UnbondingMinTime)
	if len(timeSlice) == 0 {
		k.SetValidatorQueueTimeSlice(ctx, val.UnbondingMinTime, []sdk.ValAddress{val.OperatorAddr})
	} else {
		timeSlice = append(timeSlice, val.OperatorAddr)
		k.SetValidatorQueueTimeSlice(ctx, val.UnbondingMinTime, timeSlice)
	}
}

// Returns all the validator queue timeslices from time 0 until endTime
func (k Keeper) ValidatorQueueIterator(ctx sdk.Context, endTime time.Time) sdk.Iterator {
	store := ctx.KVStore(k.storeKey)
	return store.Iterator(ValidatorQueueKey, sdk.InclusiveEndBytes(GetValidatorQueueTimeKey(endTime)))
}

// Returns a concatenated list of all the timeslices before currTime, and deletes the timeslices from the queue
func (k Keeper) GetAllMatureValidatorQueue(ctx sdk.Context, currTime time.Time) (matureValsAddrs []sdk.ValAddress) {
	// gets an iterator for all timeslices from time 0 until the current Blockheader time
	validatorTimesliceIterator := k.ValidatorQueueIterator(ctx, ctx.BlockHeader().Time)
	defer validatorTimesliceIterator.Close()

	for ; validatorTimesliceIterator.Valid(); validatorTimesliceIterator.Next() {
		timeslice := []sdk.ValAddress{}
		k.cdc.MustUnmarshalBinaryLengthPrefixed(validatorTimesliceIterator.Value(), &timeslice)
		matureValsAddrs = append(matureValsAddrs, timeslice...)
	}
	return matureValsAddrs
}

// Unbonds all the unbonding validators that have finished their unbonding period
func (k Keeper) UnbondAllMatureValidatorQueue(ctx sdk.Context) {
	store := ctx.KVStore(k.storeKey)
	validatorTimesliceIterator := k.ValidatorQueueIterator(ctx, ctx.BlockHeader().Time)
	defer validatorTimesliceIterator.Close()

	for ; validatorTimesliceIterator.Valid(); validatorTimesliceIterator.Next() {
		timeslice := []sdk.ValAddress{}
		k.cdc.MustUnmarshalBinaryLengthPrefixed(validatorTimesliceIterator.Value(), &timeslice)
		for _, valAddr := range timeslice {
			val, found := k.GetValidator(ctx, valAddr)
			if !found || !val.IsUnbonding() {
				continue
			}

			k.unbondingToUnbonded(ctx, val)
			if val.GetDelegatorShares().IsZero() {
				k.RemoveValidator(ctx, val.OperatorAddr)
			}
		}
		store.Delete(validatorTimesliceIterator.Key())
	}
}
