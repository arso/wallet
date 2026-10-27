rootProject.name = "wallet"

include(
    "libs:ledger-core",
    "libs:events",
    "libs:worker-pool",
    "wallet-api",
    "bridge",
    "mocks",
    "completion-service",
    "transfer-executor",
    "timeout-checker",
    "e2e"
)
