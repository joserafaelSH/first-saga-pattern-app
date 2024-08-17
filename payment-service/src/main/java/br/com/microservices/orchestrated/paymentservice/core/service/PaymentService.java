package br.com.microservices.orchestrated.paymentservice.core.service;


import br.com.microservices.orchestrated.paymentservice.config.exception.ValidationException;
import br.com.microservices.orchestrated.paymentservice.core.dto.Event;
import br.com.microservices.orchestrated.paymentservice.core.dto.History;
import br.com.microservices.orchestrated.paymentservice.core.enums.EPaymentStatus;
import br.com.microservices.orchestrated.paymentservice.core.enums.ESagaStatus;
import br.com.microservices.orchestrated.paymentservice.core.model.Payment;
import br.com.microservices.orchestrated.paymentservice.core.producer.kafkaProducer;
import br.com.microservices.orchestrated.paymentservice.core.repository.PaymentRepository;
import br.com.microservices.orchestrated.paymentservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
@AllArgsConstructor
public class PaymentService {
    private static final String CURRENT_SOURCE = "PAYMENT_SERVICE";
    private static final Double MIN_AMOUNT_VALUE = 0.1;

    private final JsonUtil jsonUtil;
    private final kafkaProducer producer;
    private final PaymentRepository paymentRepository;


    public void realizePayment(Event event){
        try{
            checkCurrentValidation(event);
            createPendingPayment(event);
            var payment = findByOrderIdAndTransaction(event);
            validateAmount(payment.getTotalAmount());
            changePaymentToSuccess(payment);
            handleSuccess(event);
        }
        catch (Exception ex){
            log.error("Error trying to make payment: ", ex);
            handleFailCurrentNotExecuted(event, ex.getMessage());
        }

        producer.sendEvent(jsonUtil.toJson(event));
    }

    public void realizeRefund(Event event){
        event.setSource(CURRENT_SOURCE);
        event.setStatus(ESagaStatus.FAIL);
        try{
            changePaymentStatusToRefund(event);
            addHistory(event, "Rollback executed for payment!");
        }
        catch (Exception ex){
            addHistory(event, "Rollback executed for payment!".concat(ex.getMessage()));
        }

        producer.sendEvent(jsonUtil.toJson(event));
    }

    private void changePaymentStatusToRefund(Event event){
        var payment = findByOrderIdAndTransaction(event);
        payment.setStatus(EPaymentStatus.REFUND);
        setEventAmountItems(event, payment);
        save(payment);

    }

    private void handleFailCurrentNotExecuted(Event event, String message){
        event.setStatus(ESagaStatus.ROLLBACK_PENDING);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Fail to realize payment: ".concat(message));
    }

    private void handleSuccess(Event event){
        event.setStatus(ESagaStatus.SUCCESS);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Payment realized successfully!");

    }

    private void addHistory(Event event, String message){
        var history = History.builder()
                .source(event.getSource())
                .status(event.getStatus())
                .message(message)
                .createdAt(LocalDateTime.now())
                .build();

        event.addToHistory(history);
    }

    private void changePaymentToSuccess(Payment payment){
        payment.setStatus(EPaymentStatus.SUCCESS);
        save(payment);
    }

    private void validateAmount(double amount){
        if(amount < MIN_AMOUNT_VALUE){
            throw new ValidationException("The minimum amount avaialble is ".concat(MIN_AMOUNT_VALUE.toString()));
        }
    }

    private void checkCurrentValidation(Event event){
        if(paymentRepository.existsByOrderIdAndTransactionId(event.getOrderId(), event.getTransactionId())){
            throw new ValidationException("There's another transactionId for this validation.");
        }
    }

    private void createPendingPayment(Event event){
        var totalAmount = calculateAmount(event) ;
        var totalItems = calculateTotalItems(event) ;
        var payment = Payment.builder()
                .orderId(event.getPayload().getId())
                .transactionId(event.getTransactionId())
                .totalAmount(calculateAmount(event))
                .totalItems(calculateTotalItems(event))
                .build();

        save(payment);
        setEventAmountItems(event, payment);
    }

    private double calculateAmount(Event event){
//        return event.getPayload().getProducts().stream()
//                .map(product -> product.getQuantity() * product.getProduct().getUnitValue())
//                .reduce(0.0, Double::sum);

        var products = event.getPayload().getProducts();
        var totalAmount = 0.0;
        for(int i = 0; i < products.size(); i++){
            var product = products.get(i);
            var productAmount = product.getQuantity() * product.getProduct().getUnitValue();
            totalAmount+=productAmount;
        }
        return  totalAmount;

    }

    private int calculateTotalItems(Event event){
        //        return event.getPayload().getProducts().stream()
//                .map(OrderProducts::getQuantity)
//                .reduce(0, Integer::sum);

        var products = event.getPayload().getProducts();
        var totaltems = 0;
        for(int i = 0; i < products.size(); i++){
            var product = products.get(i);
            var productQuantity = product.getQuantity();
            totaltems+=productQuantity;
        }
        return  totaltems;
    }

    private void setEventAmountItems(Event event, Payment payment){
        event.getPayload().setTotalAmount(payment.getTotalAmount());
        event.getPayload().setTotalItems(payment.getTotalItems());
    }

    private Payment findByOrderIdAndTransaction(Event event){
        return paymentRepository.findByOrderIdAndTransactionId(event.getPayload().getId(),
                event.getTransactionId())
                .orElseThrow(()-> new ValidationException("Payment not found by OrderId and TransactionId"));


    }

    private void save(Payment payment){
        paymentRepository.save(payment);
    }
}
