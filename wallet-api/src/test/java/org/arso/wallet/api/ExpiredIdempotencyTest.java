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
        String first = read(withdraw(id, key, 10).expectStatus().isAccepted(), "$.payment_id");

        Thread.sleep(2100);
        withdraw(id, key, 10)
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.payment_id").value(String.class, v -> assertThat(v).isNotEqualTo(first));

        assertThat(balance(id)).isEqualTo(80);
    }
}
