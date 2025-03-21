// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code for compiling solidity smart contracts for testing purposes.

use std::{
    fs::File,
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

use anyhow::Context;
use tempfile::tempdir;

fn write_compilation_json(path: &Path, file_name: &str) -> anyhow::Result<()> {
    let mut source = File::create(path).unwrap();
    writeln!(
        source,
        r#"
{{
  "language": "Solidity",
  "sources": {{
    "{file_name}": {{
      "urls": ["./{file_name}"]
    }}
  }},
  "settings": {{
    "viaIR": true,
    "outputSelection": {{
      "*": {{
        "*": ["evm.bytecode"]
      }}
    }}
  }}
}}
"#
    )?;
    Ok(())
}

fn get_bytecode_path(path: &Path, file_name: &str, contract_name: &str) -> anyhow::Result<Vec<u8>> {
    let config_path = path.join("config.json");
    write_compilation_json(&config_path, file_name)?;
    let config_file = File::open(config_path)?;

    let output_path = path.join("result.json");
    let output_file = File::create(output_path.clone())?;

    let status = Command::new("solc")
        .current_dir(path)
        .arg("--standard-json")
        .stdin(Stdio::from(config_file))
        .stdout(Stdio::from(output_file))
        .status()?;
    assert!(status.success());

    let contents = std::fs::read_to_string(output_path)?;
    let json_data: serde_json::Value = serde_json::from_str(&contents)?;
    let contracts = json_data
        .get("contracts")
        .context("failed to get contracts")?;
    let file_name_contract = contracts
        .get(file_name)
        .context("failed to get {file_name}")?;
    let test_data = file_name_contract
        .get(contract_name)
        .context("failed to get contract_name={contract_name}")?;
    let evm_data = test_data.get("evm").context("failed to get evm")?;
    let bytecode = evm_data.get("bytecode").context("failed to get bytecode")?;
    let object = bytecode.get("object").context("failed to get object")?;
    let object = object.to_string();
    let object = object.trim_matches(|c| c == '"').to_string();
    Ok(hex::decode(&object)?)
}

pub fn get_bytecode(source_code: &str, contract_name: &str) -> anyhow::Result<Vec<u8>> {
    let dir = tempdir().unwrap();
    let path = dir.path();
    let file_name = "test_code.sol";
    let test_code_path = path.join(file_name);
    let mut test_code_file = File::create(&test_code_path)?;
    writeln!(test_code_file, "{}", source_code)?;
    get_bytecode_path(path, file_name, contract_name)
}

pub fn get_evm_example_counter() -> anyhow::Result<Vec<u8>> {
    let source_code = r#"
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract ExampleCounter {
  uint64 value;
  constructor(uint64 start_value) {
    value = start_value;
  }

  function increment(uint64 input) external returns (uint64) {
    value = value + input;
    return value;
  }

  function get_value() external view returns (uint64) {
    return value;
  }

}
"#
    .to_string();
    get_bytecode(&source_code, "ExampleCounter")
}







pub fn get_evm_example_call_wasm_counter() -> anyhow::Result<Vec<u8>> {
    let source_code = r#"
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

function bcs_serialize_uint64(uint64 input) internal pure returns (bytes memory) {
  bytes memory result = new bytes(8);
  uint64 value = input;
  result[0] = bytes1(uint8(value));
  for (uint i=1; i<8; i++) {
    value = value >> 8;
    result[i] = bytes1(uint8(value));
  }
  return result;
}

function bcs_deserialize_offset_uint64(uint256 pos, bytes memory input) internal pure returns (uint256, uint64) {
  require(pos + 7 < input.length, "Position out of bound");
  uint64 value = uint8(input[pos + 7]);
  for (uint256 i=0; i<7; i++) {
    value = value << 8;
    value += uint8(input[pos + 6 - i]);
  }
  return (pos + 8, value);
}

contract ExampleCallWasmCounter {
  bytes32 universal_address;
  constructor(bytes32 _universal_address) {
    universal_address = _universal_address
  }

  function increment(uint64 input) external returns (uint64) {
    value = value + input;
    return value;
  }

  function get_value() external view returns (uint64) {
    return value;
  }

}
"#
    .to_string();
    get_bytecode(&source_code, "ExampleCallWasmCounter")
}




pub fn get_evm_example_call_evm_counter() -> anyhow::Result<Vec<u8>> {
    let source_code = r#"
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

interface IExternalContract {
  function increment(uint64 value) external returns (uint64);
  function get_value() external returns (uint64);
}


contract ExampleCallEvmCounter {
  address evm_address;
  constructor(address _evm_address) {
    evm_address = _evm_address;
  }
  function nest_increment(uint64 input) external returns (uint64) {
    IExternalContract externalContract = IExternalContract(evm_address);
    return externalContract.increment(input);
  }
  function nest_get_value() external view returns (uint64) {
    IExternalContract externalContract = IExternalContract(evm_address);
    return externalContract.get_value();
  }
}
"#
    .to_string();
    get_bytecode(&source_code, "ExampleCallEvmCounter")
}
