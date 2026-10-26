package org.arso.wallet.completion;

import org.arso.wallet.testing.HexagonalRules;
import org.junit.jupiter.api.Test;

class ArchitectureTest {

    @Test
    void followsHexagonalRules() {
        HexagonalRules.check("org.arso.wallet.completion");
    }
}
