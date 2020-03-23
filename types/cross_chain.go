package types

import (
	"fmt"
	"math"
	"strconv"
)

type ChannelID uint8
type CrossChainID uint16

type CrossChainChannelConfig struct {
	sourceChainID   CrossChainID
	nameToChannelID map[string]ChannelID
	channelIDToName map[ChannelID]string
	nextChannelID   ChannelID
}

var CrossChainChannelHub = newCrossChainChannelHub()

func newCrossChainChannelHub() *CrossChainChannelConfig {
	config := &CrossChainChannelConfig{
		sourceChainID:   0,
		nameToChannelID: make(map[string]ChannelID),
		channelIDToName: make(map[ChannelID]string),
		nextChannelID:   1,
	}
	return config
}

func RegisterNewCrossChainChannel(name string) error {
	_, ok := CrossChainChannelHub.nameToChannelID[name]
	if ok {
		return fmt.Errorf("duplicated channel name")
	}
	CrossChainChannelHub.nameToChannelID[name] = CrossChainChannelHub.nextChannelID
	CrossChainChannelHub.channelIDToName[CrossChainChannelHub.nextChannelID] = name
	CrossChainChannelHub.nextChannelID++
	return nil
}

func GetChannelID(channelName string) (ChannelID, error) {
	id, ok := CrossChainChannelHub.nameToChannelID[channelName]
	if !ok {
		return ChannelID(0), fmt.Errorf("non-existing channel")
	}
	return id, nil
}

func InitCrossChainID(sourceChainID CrossChainID) {
	CrossChainChannelHub.sourceChainID = sourceChainID
}

func GetSourceChainID() CrossChainID {
	return CrossChainChannelHub.sourceChainID
}

func ParseCrossChainID(input string) (CrossChainID, error) {
	chainID, err := strconv.Atoi(input)
	if err != nil {
		return CrossChainID(0), err
	}
	if chainID > math.MaxUint16 || chainID < 0 {
		return CrossChainID(0), fmt.Errorf("cross chainID must be in [0, 1<<16 - 1]")
	}
	return CrossChainID(chainID), nil
}

func (channelID ChannelID) String() string {
	return CrossChainChannelHub.channelIDToName[channelID]
}
