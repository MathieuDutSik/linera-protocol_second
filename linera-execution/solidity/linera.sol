// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

// Precompile keys:
// 0: try_call_application
// 1: try_query_application
// 2: send_message
// 3: message_id
// 4: message_is_bouncing

library Linera {
  struct MessageId {
    bool is_some;
    bytes32 chain_id;
    uint64 block_height;
    uint32 index;
  }

  enum MessageIsBouncing { NONE, IS_BOUNCING, NOT_BOUNCING }

  function try_call_application(bytes32 universal_address, bytes memory operation) internal returns (bytes memory) {
    address precompile = address(0x0b);
    bytes1 input1 = bytes1(uint8(0));
    bytes memory input2 = abi.encodePacked(input1, universal_address, operation);
    (bool success, bytes memory output) = precompile.call(input2);
    require(success);
    return output;
  }

  function try_query_application(bytes32 universal_address, bytes memory argument) internal returns (bytes memory) {
    address precompile = address(0x0b);
    bytes1 input1 = bytes1(uint8(1));
    bytes memory input2 = abi.encodePacked(input1, universal_address, argument);
    (bool success, bytes memory output) = precompile.call(input2);
    require(success);
    return output;
  }

  function send_message(bytes32 chain_id, bytes memory message) internal {
    address precompile = address(0x0b);
    bytes1 input1 = bytes1(uint8(2));
    bytes memory input2 = abi.encodePacked(input1, chain_id, message);
    (bool success, bytes memory output) = precompile.call(input2);
    require(success);
    require(output.length == 0);
  }

  function message_id() internal returns (MessageId memory) {
    address precompile = address(0x0b);
    bytes memory input1 = new bytes(1);
    input1[0] = bytes1(uint8(3));
    (bool success, bytes memory output1) = precompile.call(input1);
    require(success);
    MessageId memory output2 = abi.decode(output1, (MessageId));
    return output2;
  }

  function message_is_bouncing() internal returns (MessageIsBouncing) {
    address precompile = address(0x0b);
    bytes memory input1 = new bytes(1);
    input1[0] = bytes1(uint8(4));
    (bool success, bytes memory output1) = precompile.call(input1);
    require(success);
    MessageIsBouncing output2 = abi.decode(output1, (MessageIsBouncing));
    return output2;
  }
}
