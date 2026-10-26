package org.arso.wallet.mocks.application;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.LedgerEvent;
import org.arso.wallet.mocks.port.WalletAdmin;
import org.arso.wallet.mocks.port.WalletAdmin.Result;

public final class AmlMonitor {

    private enum Action {
        BLOCK,
        UNBLOCK
    }

    private final WalletAdmin wallet;
    private final long blockThreshold;
    // TODO(later): in memory only; a restart forgets actions waiting for a release.
    private final Map<String, Action> waiting = new ConcurrentHashMap<>();

    public AmlMonitor(WalletAdmin wallet, long blockThreshold) {
        this.wallet = wallet;
        this.blockThreshold = blockThreshold;
    }

    public void on(LedgerEvent event) {
        var key = "aml-" + event.accountId() + "-" + event.seq();
        switch (event.eventType()) {
            case "deposit" -> {
                if (event.amount() != null && event.amount() >= blockThreshold) {
                    apply(event.accountId(), Action.BLOCK, "large deposit", key);
                } else if (event.externalTxId() != null && event.externalTxId().startsWith("aml-unblock")) {
                    apply(event.accountId(), Action.UNBLOCK, "cleared", key);
                }
            }
            case Events.ACCOUNT_RELEASED -> {
                var wanted = waiting.remove(event.accountId());
                var already = wanted == Action.BLOCK ? "BLOCKED" : "ACTIVE";
                if (wanted != null && !already.equals(event.releasedTo())) {
                    apply(event.accountId(), wanted, "applied after release", key);
                }
            }
            default -> {}
        }
    }

    private void apply(String accountId, Action action, String reason, String key) {
        var result = action == Action.BLOCK
                ? wallet.block(accountId, reason, key)
                : wallet.unblock(accountId, reason, key);
        if (result == Result.ON_HOLD) {
            waiting.put(accountId, action);
        }
    }
}
