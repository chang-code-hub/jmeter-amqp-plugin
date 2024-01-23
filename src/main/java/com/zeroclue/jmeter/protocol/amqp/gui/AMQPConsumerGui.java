package com.zeroclue.jmeter.protocol.amqp.gui;

import com.zeroclue.jmeter.protocol.amqp.AMQPConsumer;

import javax.swing.*;

import com.zeroclue.jmeter.protocol.amqp.AMQPPublisher;
import org.apache.jmeter.gui.util.HorizontalPanel;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jorphan.gui.JLabeledTextField;

import java.awt.*;

/**
 * GUI for AMQP Consumer.
 */
public class AMQPConsumerGui extends AMQPSamplerGui {

    private static final long serialVersionUID = 1L;

    private final JLabeledTextField receiveTimeout = new JLabeledTextField("Receive Timeout");
    private final JLabeledTextField prefetchCount = new JLabeledTextField("   Prefetch Count");

    private final JLabeledTextField messageId = new JLabeledTextField("           Message ID");
    private final JLabeledTextField correlationId = new JLabeledTextField("       Correlation ID");

    private final JCheckBox purgeQueue = new JCheckBox("Purge Queue", AMQPConsumer.DEFAULT_PURGE_QUEUE);
    private final JCheckBox autoAck = new JCheckBox("Auto ACK", AMQPConsumer.DEFAULT_AUTO_ACK);
    private final JCheckBox readResponse = new JCheckBox("Read Response", AMQPConsumer.DEFAULT_READ_RESPONSE);
    private final JCheckBox useTx = new JCheckBox("Use Transactions", AMQPConsumer.DEFAULT_USE_TX);

    private static final String MSG_SETTINGS_LABEL = "Message";
    private static final String PROPS_SETTINGS_LABEL = "Properties";
    private JPanel mainPanel;

    public AMQPConsumerGui() {
        init();
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public String getLabelResource() {
        return this.getClass().getSimpleName();
    }

    @Override
    public String getStaticLabel() {
        return "AMQP Consumer";
    }

    /*
     * Helper method to set up the GUI screen
     */
    @Override
    protected void init() {
        super.init();

        JPanel optionsPanel = new HorizontalPanel();
        optionsPanel.add(purgeQueue);
        optionsPanel.add(autoAck);
        optionsPanel.add(readResponse);
        optionsPanel.add(useTx);
        optionsPanel.add(messageId);
        optionsPanel.add(correlationId);

        mainPanel.add(receiveTimeout);
        mainPanel.add(prefetchCount);
        mainPanel.add(optionsPanel);

        optionsPanel.setPreferredSize(optionsPanel.getPreferredSize());
        optionsPanel.validate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(TestElement element) {
        super.configure(element);
        if (!(element instanceof AMQPConsumer)) {
            return;
        }
        AMQPConsumer sampler = (AMQPConsumer) element;

        readResponse.setSelected(sampler.getReadResponseAsBoolean());
        prefetchCount.setText(sampler.getPrefetchCount());
        receiveTimeout.setText(sampler.getReceiveTimeout());
        purgeQueue.setSelected(sampler.purgeQueue());
        autoAck.setSelected(sampler.autoAck());
        useTx.setSelected(sampler.getUseTx());

        correlationId.setText(sampler.getCorrelationId());
        messageId.setText(sampler.getMessageId());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearGui() {
        super.clearGui();
        readResponse.setSelected(AMQPConsumer.DEFAULT_READ_RESPONSE);
        prefetchCount.setText(AMQPConsumer.DEFAULT_PREFETCH_COUNT_STRING);
        useTx.setSelected(AMQPConsumer.DEFAULT_USE_TX);
        receiveTimeout.setText(AMQPConsumer.DEFAULT_RECEIVE_TIMEOUT);
        purgeQueue.setSelected(AMQPConsumer.DEFAULT_PURGE_QUEUE);
        autoAck.setSelected(AMQPConsumer.DEFAULT_AUTO_ACK);
        correlationId.setText("");
        messageId.setText("");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TestElement createTestElement() {
        AMQPConsumer sampler = new AMQPConsumer();
        modifyTestElement(sampler);

        return sampler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void modifyTestElement(TestElement te) {
        AMQPConsumer sampler = (AMQPConsumer) te;
        sampler.clear();
        configureTestElement(sampler);

        super.modifyTestElement(sampler);

        sampler.setReadResponse(readResponse.isSelected());
        sampler.setPrefetchCount(prefetchCount.getText());
        sampler.setReceiveTimeout(receiveTimeout.getText());
        sampler.setPurgeQueue(purgeQueue.isSelected());
        sampler.setAutoAck(autoAck.isSelected());
        sampler.setUseTx(useTx.isSelected());
        sampler.setCorrelationId(correlationId.getText());
        sampler.setMessageId(messageId.getText());
    }

    @Override
    protected void setMainPanel(JPanel panel) {
        mainPanel = panel;
    }
}
