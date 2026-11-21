package com.tianji.trade.service.impl;

import com.tianji.trade.service.PayQueryScheduler;
import com.tianji.pay.sdk.client.PayClient;
import com.tianji.pay.sdk.dto.PayResultDTO;
import com.tianji.trade.config.TradeProperties;
import com.tianji.trade.constants.OrderStatus;
import com.tianji.trade.domain.dto.OrderDelayQueryDTO;
import com.tianji.trade.domain.po.Order;
import com.tianji.trade.service.IOrderDetailService;
import com.tianji.trade.service.IOrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PayServiceImplTest {

    private PayClient payClient;
    private IOrderService orderService;
    private PayQueryScheduler mq;
    private PayServiceImpl service;

    @BeforeEach
    void setUp() {
        payClient = mock(PayClient.class);
        orderService = mock(IOrderService.class);
        mq = mock(PayQueryScheduler.class);
        service = new PayServiceImpl(payClient, orderService, mock(IOrderDetailService.class), new TradeProperties(), mq);
        when(orderService.getById(1L)).thenReturn(new Order().setId(1L).setStatus(OrderStatus.NO_PAY.getValue()));
    }

    @Test
    void pendingPaymentSchedulesTheNextQueryWithoutMarkingTheOrderPaidOrMutatingTheInput() {
        when(payClient.queryPayResult(1L)).thenReturn(PayResultDTO.builder().status(PayResultDTO.PAYING).build());
        OrderDelayQueryDTO message = OrderDelayQueryDTO.init(1L);
        List<Long> originalDelays = new ArrayList<>(message.getDelayMillis());
        service.queryPayResult(message);
        service.queryPayResult(message);
        verify(orderService, never()).handlePaySuccess(any());
        assertEquals(originalDelays, message.getDelayMillis());
        verify(mq, times(2)).start(1L);
    }

    @Test
    void exhaustedFailedPaymentDoesNotScheduleOrMarkPaid() {
        when(payClient.queryPayResult(1L)).thenReturn(PayResultDTO.builder().status(PayResultDTO.FAILED).build());
        OrderDelayQueryDTO message = OrderDelayQueryDTO.init(1L);
        message.setDelayMillis(Collections.emptyList());
        service.queryPayResult(message);
        verify(orderService, never()).handlePaySuccess(any());
        verifyNoInteractions(mq);
    }

    @Test
    void onlyASuccessfulQueryCallsTheOptimisticPaymentHandler() {
        PayResultDTO result = PayResultDTO.builder().status(PayResultDTO.SUCCESS).build();
        when(payClient.queryPayResult(1L)).thenReturn(result);
        service.queryPayResult(OrderDelayQueryDTO.init(1L));
        verify(orderService).handlePaySuccess(result);
        verifyNoInteractions(mq);
    }

    @Test
    void anAlreadyPaidOrderDoesNotQueryAgain() {
        when(orderService.getById(1L)).thenReturn(new Order().setId(1L).setStatus(OrderStatus.PAYED.getValue()));
        service.queryPayResult(OrderDelayQueryDTO.init(1L));
        verifyNoInteractions(payClient, mq);
        verify(orderService, never()).handlePaySuccess(any());
    }

    @Test
    void stalePersistentRoundDoesNotQueryOrScheduleAgain() {
        OrderDelayQueryDTO message = persistentMessage();
        when(mq.isCurrent(message)).thenReturn(false);
        service.queryPayResult(message);
        verifyNoInteractions(payClient, orderService);
        verify(mq, never()).advance(any());
    }

    @Test
    void currentPersistentPendingRoundAdvancesWithoutMutatingTheInput() {
        OrderDelayQueryDTO message = persistentMessage();
        when(mq.isCurrent(message)).thenReturn(true);
        when(payClient.queryPayResult(1L)).thenReturn(PayResultDTO.builder().status(PayResultDTO.PAYING).build());
        service.queryPayResult(message);
        verify(mq).advance(message);
        assertEquals(0, message.getQueryRound());
        verify(orderService, never()).handlePaySuccess(any());
    }

    @Test
    void invalidPersistentMetadataIsRejected() {
        OrderDelayQueryDTO message = persistentMessage();
        message.setQueryRound(null);
        assertThrows(com.tianji.common.exceptions.BadRequestException.class, () -> service.queryPayResult(message));
        verifyNoInteractions(mq, payClient);
    }

    private OrderDelayQueryDTO persistentMessage() {
        OrderDelayQueryDTO message = OrderDelayQueryDTO.init(1L);
        message.setQueryGeneration(1L);
        message.setQueryRound(0);
        return message;
    }
}
