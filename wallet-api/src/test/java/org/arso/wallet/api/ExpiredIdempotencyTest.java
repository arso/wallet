package org.arso.wallet.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(properties = "wallet.idempotency.ttl=1s")
class ExpiredIdempotencyTest extends ApiTest {

    @Test
    void expiredButNotDeletedKeyIsTreatedAsNew() throws Exception {
        var id = account(100);
        var key = UUID.randomUUID().toString();
        var first = withdraw(id, key, 10);

        Thread.sleep(2100);
        var second = withdraw(id, key, 10);

        assertThat(second.status()).isEqualTo(202);
        assertThat(second.text("payment_id")).isNotEqualTo(first.text("payment_id"));
        assertThat(balance(id)).isEqualTo(80);
    }
}
