package org.sdf.rkm.aggex.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.integration.store.MessageGroupStoreReaper;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

@Configuration
public class MessageGroupStoreReaperRunner {
    private final List<MessageGroupStoreReaper> reapers;

    MessageGroupStoreReaperRunner(List<MessageGroupStoreReaper> reapers) {
        this.reapers = reapers;
    }

    @Scheduled(fixedRate = 10000)
    public void runMessageGroupStoreReaper() {
        reapers.forEach(MessageGroupStoreReaper::run);
    }
}