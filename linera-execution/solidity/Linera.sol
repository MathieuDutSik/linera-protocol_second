// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "./LineraTypes.sol";

// This library provides Linera functionalities to EVM contracts
// It should not be modified.

// The Precompile keys below correspond to the BCS serialization of
// the `PrecompileTag` in `linera-execution/src/evm/revm.rs`.
// (0,0): chain_id
// (0,1): application_creator_chain_id
// (0,2): chain_ownership
// (0,3): read data blob
// (0,4): assert data blob exists
// (1,0): try_call_application
// (1,1): validation round
// (1,2): send_message
// (1,3): message_id
// (1,4): message_is_bouncing
// (1,5): reading events in the stream.
// (1,6): subscribe_to_events in a stream.
// (1,7): unsubscribe_from_events in a stream.
// (2,0): try_query_application
library Linera {

    function inner_chain_id(uint8 val) internal returns (LineraTypes.ChainId memory) {
        address precompile = address(0x0b);
        bytes memory input = abi.encodePacked(uint8(0), val);
        (bool success, bytes memory output) = precompile.call(input);
        require(success);
        return LineraTypes.bcs_deserialize_ChainId(output);
    }

    function chain_id() internal returns (LineraTypes.ChainId memory) {
        return inner_chain_id(uint8(0));
    }

    function application_creator_chain_id() internal returns (LineraTypes.ChainId memory) {
        return inner_chain_id(uint8(1));
    }

    function chain_ownership() internal returns (LineraTypes.ChainOwnership memory) {
        address precompile = address(0x0b);
        bytes memory input = abi.encodePacked(uint8(0), uint8(2));
        (bool success, bytes memory output) = precompile.call(input);
        require(success);
        return LineraTypes.bcs_deserialize_ChainOwnership(output);
    }

    function read_data_blob(bytes32 hash) internal returns (bytes memory) {
        address precompile = address(0x0b);
        bytes memory input = abi.encodePacked(uint8(0), uint8(3), hash);
        (bool success, bytes memory output) = precompile.call(input);
        require(success);
        return output;
    }

    function assert_data_blob_exists(bytes32 hash) internal {
        address precompile = address(0x0b);
        bytes memory input = abi.encodePacked(uint8(0), uint8(4), hash);
        (bool success, bytes memory output) = precompile.call(input);
        require(success);
        assert(output.length == 0);
    }

    function try_call_application(bytes32 universal_address, bytes memory operation) internal returns (bytes memory) {
        address precompile = address(0x0b);
        bytes memory input = abi.encodePacked(uint8(1), uint8(0), universal_address, operation);
        (bool success, bytes memory output) = precompile.call(input);
        require(success);
        return output;
    }

    function validation_round() internal returns (LineraTypes.opt_uint32 memory) {
        address precompile = address(0x0b);
        bytes memory input = abi.encodePacked(uint8(1), uint8(1));
        (bool success, bytes memory output) = precompile.call(input);
        require(success);
        return LineraTypes.bcs_deserialize_opt_uint32(output);
    }

    function send_message(bytes32 input_chain_id, bytes memory message) internal {
        address precompile = address(0x0b);
        bytes memory input = abi.encodePacked(uint8(1), uint8(2), input_chain_id, message);
        (bool success, bytes memory output) = precompile.call(input);
        require(success);
        require(output.length == 0);
    }

    function message_id() internal returns (LineraTypes.opt_MessageId memory) {
        address precompile = address(0x0b);
        bytes memory input = abi.encodePacked(uint8(1), uint8(3));
        (bool success, bytes memory output) = precompile.call(input);
        require(success);
        return LineraTypes.bcs_deserialize_opt_MessageId(output);
    }

    function message_is_bouncing() internal returns (LineraTypes.MessageIsBouncing memory) {
        address precompile = address(0x0b);
        bytes memory input = abi.encodePacked(uint8(1), uint8(4));
        (bool success, bytes memory output) = precompile.call(input);
        require(success);
        return abi.decode(output, (LineraTypes.MessageIsBouncing));
    }

    function read_event(bytes32 chain_id, bytes memory stream_name, uint32 index) internal returns (bytes memory) {
        bytes memory input = abi.encodePacked(uint8(1), uint8(5), chain_id, stream_name, index);
        address precompile = address(0x0b);
        (bool success, bytes memory output) = precompile.call(input);
        require(success);
        return output;
    }

    function subscribe_to_events(bytes32 chain_id, bytes32 application_id, bytes memory stream_name) internal {
        bytes memory input = abi.encodePacked(uint8(1), uint8(6), chain_id, application_id, stream_name);
        address precompile = address(0x0b);
        (bool success, bytes memory output) = precompile.call(input);
        require(success);
        require(output.length == 0);
    }

    function unsubscribe_from_events(bytes32 chain_id, bytes32 application_id, bytes memory stream_name) internal {
        bytes memory input = abi.encodePacked(uint8(1), uint8(7), chain_id, application_id, stream_name);
        address precompile = address(0x0b);
        (bool success, bytes memory output) = precompile.call(input);
        require(success);
        require(output.length == 0);
    }

    function try_query_application(bytes32 universal_address, bytes memory argument) internal returns (bytes memory) {
        address precompile = address(0x0b);
        bytes memory input2 = abi.encodePacked(uint8(2), uint8(0), universal_address, argument);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return output;
    }
}
