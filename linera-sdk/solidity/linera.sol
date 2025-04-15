// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

// Precompile keys:
// 0: send_message
// 1: message_id
// 2: try_call_application

library Linera {
  struct MessageId {
    bool is_some;
    bytes32 chain_id;
    uint64 block_height;
    uint32 index;
  }

  function send_message(bytes32 chain_id, bytes message) internal pure {
    address precompile = address(0x0b);
    bytes1 input1 = bytes1(0);
    bytes memory input2 = abi.encodePacked(input1, chain_id, message);
    (bool success, bytes memory output) = precompile.call(input2);
    require(success);
    require(output.length == 0);
  }

  function message_id() internal pure returns (MessageId) {
    address precompile = address(0x0b);
    bytes1 input1 = bytes1(1);
    (bool success, bytes memory output1) = precompile.call(input2);
    require(success);
    MessageId output2 = abi.decode(output1, (MessageId));
    return output2;
  }

  function try_call_application(bytes32 address, bytes memory operation) internal pure returns (bytes) {
    address precompile = address(0x0b);
    bytes1 input1 = bytes1(2);
    bytes memory input2 = abi.encodePacked(input1, address, operation);
    (bool success, bytes memory output) = precompile.call(input2);
    require(success);
    return output;
  }
}

