package com.amedigital.incentivo.service.sqs.listener;

import com.amedigital.incentivo.configuration.ApplicationConfig;
import com.amedigital.incentivo.dto.incentive.request.IncentiveRequestDTO;
import com.amedigital.incentivo.model.enums.IncentiveStatus;
import com.amedigital.incentivo.service.IncentiveService;
import com.amedigital.incentivo.service.LoginService;
import com.amedigital.incentivo.service.sqs.SqsListener;
import com.amedigital.incentivo.util.JsonUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class SqsCreateIncentiveListener implements SqsListener {
    private final LoginService loginService;
    private final SqsAsyncClient sqsAsyncClient;
    private final IncentiveService incentiveService;
    private final ApplicationConfig applicationConfig;

    private final List<IncentiveStatus> ALLOWED_STATUS = List.of(IncentiveStatus.PENDING);

    @Override
    @Scheduled(fixedRate = 1000)
    public Disposable processMessage() {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .maxNumberOfMessages(applicationConfig.getMaxReceiveCount())
                .queueUrl(applicationConfig.getCreateIncentiveQueueUrl())
                .build();

        return Mono.fromFuture(sqsAsyncClient.receiveMessage(receiveMessageRequest))
                .flatMapIterable(ReceiveMessageResponse::messages)
                .flatMap(message -> {
                    log.info("Iniciando o processamento da mensagem. messageBody: {}", message.body());

                    var messageRequest = JsonUtil.fromJson(message.body(), IncentiveRequestDTO.class);

                    return execute(messageRequest)
                            .filter(Boolean::booleanValue)
                            .flatMap(c -> {
                                log.info("Deletando a mensagem da fila. message: [{}]", message);
                                sqsAsyncClient.deleteMessage(
                                        DeleteMessageRequest.builder()
                                                .queueUrl(applicationConfig.getCreateIncentiveQueueUrl())
                                                .receiptHandle(message.receiptHandle())
                                                .build()
                                );
                                return Mono.empty();
                            })
                            .doOnError(throwable -> log.warn("Ocorreu algum erro no momento do processamento do incentivo. errorMessage: {}", throwable.getMessage()));
                })
                .subscribe();
    }

    private Mono<Boolean> execute(IncentiveRequestDTO message) {
        return loginService.getApplicationToken()
                .flatMap(tokenResponse -> incentiveService.createIncentive(message, Boolean.FALSE, tokenResponse.getAccessToken()))
                .flatMap(incentiveResponseDTO -> (ALLOWED_STATUS.contains(incentiveResponseDTO.getIncentiveStatus())) ? Mono.just(true) : Mono.just(false));
    }
}

