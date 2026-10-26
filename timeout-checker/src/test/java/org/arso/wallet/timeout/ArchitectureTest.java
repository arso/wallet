package org.arso.wallet.timeout;

import org.arso.wallet.testing.HexagonalRules;
import org.junit.jupiter.api.Test;

class ArchitectureTest {

    @Test
    void followsHexagonalRules() {
        HexagonalRules.check("org.arso.wallet.timeout");
    }
}
