// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.28;

// The compiled creation bytecode lives in StandardValidatorContract.bin (loaded by
// E2eContractDeployer). After editing this file, regenerate it from this resources directory
// with:
//   docker run --rm -v "$PWD":/src ethereum/solc:0.8.28 \
//     --bin --optimize --optimize-runs 200 /src/StandardValidatorContract.sol \
//     | awk '/^Binary:/{getline; print}' | tr -d '\n' > StandardValidatorContract.bin

/// @title StandardValidatorContract
/// @notice A minimal Besu QBFT "contract" validator-selection source, matching the standard
///         storage layout `QbftContractValidatorSource.sol` (clpr-smart-contracts) and
///         `StandardValidatorContractProofSource` (clpr-relay-evm) both assume: a plain
///         `address[]` state variable, with `getValidators()` per Besu's
///         `ValidatorSmartContractInterface`.
/// @dev E2E-only fixture, not a production contract — real deployments should follow the
///      Consensys validator-smart-contracts reference implementation (governed membership,
///      access control on rotation) rather than this unrestricted stand-in.
contract StandardValidatorContract {
    /// @dev Slot 0 — the only state variable, so the array's own storage slot is 0 and its
    ///      elements live at keccak256(0) + i.
    address[] public validators;

    constructor(address[] memory initialValidators) {
        validators = initialValidators;
    }

    /// @notice Besu's contract-mode validator source interface.
    function getValidators() external view returns (address[] memory) {
        return validators;
    }

    /// @notice Rotates the validator set. Unrestricted — an E2E-only fixture, not for production use.
    function setValidators(address[] memory newValidators) external {
        validators = newValidators;
    }
}
