package org.arso.wallet.api.adapter.beneficiary;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Optional;
import org.arso.wallet.api.port.BeneficiaryLookup;
import org.arso.wallet.api.port.BeneficiaryUnavailableException;
import org.arso.wallet.ledger.domain.Counterparty;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

// TODO: cap concurrent calls to the beneficiary service (spec 4, virtual threads notes).
public final class HttpBeneficiaryLookup implements BeneficiaryLookup {

    record Beneficiary(String beneficiaryId, String name, String accountMasked, String accountFingerprint, String country) {}

    private final RestClient client;
    private final JsonMapper mapper;

    public HttpBeneficiaryLookup(URI baseUrl, Duration timeout, JsonMapper mapper) {
        var factory = new JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(timeout).build());
        factory.setReadTimeout(timeout);
        this.client = RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
        this.mapper = mapper;
    }

    @Override
    public Optional<Counterparty> find(String beneficiaryId) {
        try {
            var body = client.get().uri("/beneficiaries/{id}", beneficiaryId).exchange((request, response) -> {
                if (response.getStatusCode().value() == 404) {
                    return null;
                }
                if (!response.getStatusCode().is2xxSuccessful()) {
                    throw new BeneficiaryUnavailableException("beneficiary service returned " + response.getStatusCode(), null);
                }
                return response.bodyTo(String.class);
            });
            return Optional.ofNullable(body)
                    .map(json -> mapper.readValue(json, Beneficiary.class))
                    .map(b -> new Counterparty(b.beneficiaryId, b.name, b.accountMasked, b.accountFingerprint, b.country));
        } catch (RestClientException | JacksonException e) {
            throw new BeneficiaryUnavailableException("beneficiary lookup failed", e);
        }
    }
}
