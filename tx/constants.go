package tx

const (
	MODULE_BANK         = "bank"
	MODULE_STAKING      = "staking"
	MODULE_DISTRIBUTION = "distribution"
	MODULE_GOVERNANCE   = "governance"
	MODULE_SLASHING     = "slashing"
	MODULE_PAYMENT      = "payment"
	MODULE_WASM         = "wasm"
	MODULE_GAS_TRACKER  = "gastracker"

	ACTION_CREATE_VALIDATOR          = "/cosmos.staking.v1beta1.MsgCreateValidator"              // "create_validator"
	ACTION_SEND                      = "/cosmos.bank.v1beta1.MsgSend"                            // "send"
	ACTION_DELEGATE                  = "/cosmos.staking.v1beta1.MsgDelegate"                     // "delegate"
	ACTION_BEGIN_REDELEGATE          = "/cosmos.staking.v1beta1.MsgBeginRedelegate"              // "begin_redelegate"
	ACTION_BEGIN_UNBONDING           = "/cosmos.staking.v1beta1.MsgUndelegate"                   // "begin_unbonding"
	ACTION_WITHDRAW_DELEGATOR_REWARD = "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward" // "withdraw_delegator_reward"
	ACTION_SUBMIT_PROPOSAL           = "/cosmos.gov.v1beta1.MsgSubmitProposal"                   // "submit_proposal"
	ACTION_VOTE                      = "/cosmos.gov.v1.MsgVote"                                  // "vote"
	ACTION_UNJAIL                    = "/cosmos.slashing.v1beta1.MsgUnjail"                      // "unjail"
	ACTION_PFD                       = "/payment.MsgPayForData"                                  // "pfd"
	ACTION_PFB                       = "/celestia.blob.v1.MsgPayForBlobs"                        // "pfb"

	ACTION_STORE_CODE            = "/cosmwasm.wasm.v1.MsgStoreCode"
	ACTION_INSTANTIATE_CONTRACT  = "/cosmwasm.wasm.v1.MsgInstantiateContract"
	ACTION_SET_CONTRACT_METADATA = "/archway.gastracker.v1.MsgSetContractMetadata"
	ACTION_EXECUTE_CONTRACT      = "/cosmwasm.wasm.v1.MsgExecuteContract"
)
