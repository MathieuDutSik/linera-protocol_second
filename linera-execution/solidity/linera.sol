// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

// Precompile keys:
// 0: send_message
// 1: message_id
// 2: is_bouncing
// 3: try_call_application
// 4: try_query_application

library Linera {
  struct MessageId {
    bool is_some;
    bytes32 chain_id;
    uint64 block_height;
    uint32 index;
  }

  enum MessageIsBouncing { NONE, IS_BOUNCING, NOT_BOUNCING }

  function send_message(bytes32 chain_id, bytes message) internal {
    address precompile = address(0x0b);
    bytes1 input1 = bytes1(0);
    bytes memory input2 = abi.encodePacked(input1, chain_id, message);
    (bool success, bytes memory output) = precompile.call(input2);
    require(success);
    require(output.length == 0);
  }

  function message_id() internal returns (MessageId) {
    address precompile = address(0x0b);
    bytes1 input1 = bytes1(1);
    (bool success, bytes memory output1) = precompile.call(input1);
    require(success);
    MessageId output2 = abi.decode(output1, (MessageId));
    return output2;
  }

  function message_is_bouncing() internal returns (MessageIsBouncing) {
    address precompile = address(0x0b);
    bytes1 input1 = bytes1(2);
    (bool success, bytes memory output1) = precompile.call(input1);
    require(success);
    MessageIsBouncing output2 = abi.decode(output1, (MessageIsBouncing));
    return output2;
  }

  function try_call_application(bytes32 address, bytes memory operation) internal pure returns (bytes) {
    address precompile = address(0x0b);
    bytes1 input1 = bytes1(3);
    bytes memory input2 = abi.encodePacked(input1, address, operation);
    (bool success, bytes memory output) = precompile.call(input2);
    require(success);
    return output;
  }

  function try_query_application(bytes32 address, bytes memory argument) internal pure returns (bytes) {
    address precompile = address(0x0b);
    bytes1 input1 = bytes1(4);
    bytes memory input2 = abi.encodePacked(input1, address, argument);
    (bool success, bytes memory output) = precompile.call(input2);
    require(success);
    return output;
  }

}
